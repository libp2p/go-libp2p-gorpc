// Package rpc is heavily inspired by Go standard net/rpc package. It aims to
// do the same thing, except it uses Libp2p for communication and provides
// context support for cancelling operations.
//
// A server registers an object, making it visible as a service with the name of
// the type of the object.  After registration, exported methods of the object
// will be accessible remotely.  A server may register multiple objects
// (services) of different types but it is an error to register multiple objects
// of the same type.
//
// Only methods that satisfy these criteria will be made available for remote
// access; other methods will be ignored:
// 	- the method's type is exported.
// 	- the method is exported.
// 	- the method has 3 arguments.
// 	- the method's first argument is a context.
// 	- the method's second are third arguments are both exported (or builtin) types.
// 	- the method's second argument is a pointer.
// 	- the method has return type error.
//
// In effect, the method must look schematically like
//
// 	func (t *T) MethodName(ctx context.Context, argType T1, replyType *T2) error
//
// where T1 and T2 can be marshaled by encoding/gob.
//
// The method's first argument represents the arguments provided by the caller;
// the second argument represents the result parameters to be returned to the
// caller.  The method's return value, if non-nil, is passed back as a string
// that the client sees as if created by errors.New.  If an error is returned,
// the reply parameter may not be sent back to the client.
//
// In order to use this package, a ready-to-go LibP2P Host must be provided
// to clients and servers, along with a protocol.ID. rpc will add a stream
// handler for the given protocol. Hosts must be ready to speak to clients,
// that is, peers must be part of the peerstore along with keys if secio
// communication is required.
//
// Since version 2.0.0, contexts are supported and honored. On the server side,
// methods must take a context. A closure or reset of the libp2p stream will
// trigger a cancellation of the context received by the functions.
// On the client side, the user can optionally provide a context.
// Cancelling the client's context will cancel the operation both on the
// client and on the server side (by closing the associated stream).
package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	logging "github.com/ipfs/go-log"
	stats "github.com/libp2p/go-libp2p-gorpc/stats"
)

var logger = logging.Logger("p2p-gorpc")

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
}

// service stores information about a service (which is a pointer to a
// Go struct normally)
type service struct {
	name   string                 // name of service
	rcvr   reflect.Value          // receiver of methods for the service
	typ    reflect.Type           // type of the receiver
	method map[string]*methodType // registered methods
}

// ServiceID is a header sent when performing an RPC request
// which identifies the service and method being called.
type ServiceID struct {
	Name   string
	Method string
}

// Response is a header sent when responding to an RPC
// request which includes any error that may have happened.
type Response struct {
	Service ServiceID
	Error   string // error, if any.
	ErrType responseErr
}

// AuthorizeWithMap returns an authrorization function that follows the
// strategy as described in the given map(maps "service.method" of a peer to
// boolean permission).
func AuthorizeWithMap(p map[peer.ID]map[string]bool) func(pid peer.ID, svc string, method string) bool {
	return func(pid peer.ID, svc string, method string) bool {
		// If map is nil, no method would be allowed
		if p == nil {
			return false
		}
		return p[pid][svc+"."+method]
	}
}

// WithAuthorizeFunc adds authorization strategy(A function defining whether
// the given peer id is allowed to access given method of the given service)
// to the server using given authorization function.
func WithAuthorizeFunc(a func(pid peer.ID, name string, method string) bool) ServerOption {
	return func(s *Server) {
		s.authorize = a
	}
}

// ServerOption allows for functional setting of options on a Server.
type ServerOption func(*Server)

// WithServerStatsHandler providers a implementation of stats.Handler to be
// used by the Server.
func WithServerStatsHandler(h stats.Handler) ServerOption {
	return func(s *Server) {
		s.statsHandler = h
	}
}

// Server is an LibP2P RPC server. It can register services which comply to the
// limitations outlined in the package description and it will call the relevant
// methods when receiving requests from a Client.
//
// A server needs a LibP2P host and a protocol, which must match the one used
// by the client. The LibP2P host must be already correctly configured to
// be able to handle connections from clients.
type Server struct {
	host         host.Host
	protocol     protocol.ID
	statsHandler stats.Handler

	mu         sync.RWMutex // protects the serviceMap
	serviceMap map[string]*service

	// authorize defines authorization strategy of the server
	// If Authorization function is not provided, all methods would be allowed.
	authorize func(peer.ID, string, string) bool
}

// NewServer creates a Server object with the given LibP2P host
// and protocol.
func NewServer(h host.Host, p protocol.ID, opts ...ServerOption) *Server {
	s := &Server{
		host:     h,
		protocol: p,
	}

	for _, opt := range opts {
		opt(s)
	}

	if h != nil {
		h.SetStreamHandler(p, func(stream network.Stream) {
			sWrap := wrapStream(stream)
			defer helpers.FullClose(stream)
			err := s.handle(sWrap)
			if err != nil {
				logger.Error("error handling RPC:", err)
				resp := &Response{ServiceID{}, err.Error(), responseErrorType(err)}
				sendResponse(sWrap, resp, nil)
			}
		})
	}
	return s
}

// ID returns the peer.ID of the host associated with this server.
func (server *Server) ID() peer.ID {
	if server.host == nil {
		return ""
	}
	return server.host.ID()
}

func (server *Server) handle(s *streamWrap) error {
	logger.Debugf("%s: handling remote RPC from %s", server.host.ID().Pretty(), s.stream.Conn().RemotePeer())
	var err error
	var svcID ServiceID
	var argv, replyv reflect.Value
	ctx := context.Background()

	err = s.dec.Decode(&svcID)
	if err != nil {
		return newServerError(err)
	}

	sh := server.statsHandler
	if sh != nil {
		ctx = sh.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/" + svcID.Name + "/" + svcID.Method})
		beginTime := time.Now()
		begin := &stats.Begin{
			BeginTime: beginTime,
		}
		sh.HandleRPC(ctx, begin)

		defer func() {
			end := &stats.End{
				BeginTime: beginTime,
				EndTime:   time.Now(),
			}
			if err != nil && err != io.EOF {
				end.Error = newServerError(err)
			}
			sh.HandleRPC(ctx, end)
		}()
	}

	logger.Debugf("RPC ServiceID is %s.%s", svcID.Name, svcID.Method)

	service, mtype, err := server.getService(svcID)
	if err != nil {
		return newServerError(err)
	}

	if server.authorize != nil && !server.authorize(s.stream.Conn().RemotePeer(), svcID.Name, svcID.Method) {
		errMsg := fmt.Sprintf("client does not have permissions to this method, service name: %s, method name: %s", svcID.Name, svcID.Method)
		return newAuthorizationError(errors.New(errMsg))
	}

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argIsValue = true
	}
	// argv guaranteed to be a pointer now.
	if err = s.dec.Decode(argv.Interface()); err != nil {
		return newServerError(err)
	}
	if argIsValue {
		argv = argv.Elem()
	}

	replyv = reflect.New(mtype.ReplyType.Elem())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ctxv := reflect.ValueOf(ctx)

	// TODO(lanzafame): once I figure out a
	// good to get the size of the payload.
	// inPayload := &stats.InPayload{
	// 	Length:   ,
	// 	RecvTime: beginTime,
	// }
	// sh.HandleRPC(ctx, inPayload)

	// This is a connection watchdog. We do not
	// need to read from this stream anymore.
	// However we'd like to know if the other side is closed
	// (or reset). In that case, we need to cancel our
	// context. Note this will also happen at the end
	// of a successful operation when we close the stream
	// on our side.
	go func() {
		p := make([]byte, 1)
		_, err := s.stream.Read(p)
		if err != nil {
			cancel()
		}
	}()

	// Call service and respond
	return service.svcCall(s, mtype, svcID, ctxv, argv, replyv)
}

// svcCall calls the actual method associated
func (s *service) svcCall(sWrap *streamWrap, mtype *methodType, svcID ServiceID, ctxv, argv, replyv reflect.Value) error {
	function := mtype.method.Func

	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call([]reflect.Value{s.rcvr, ctxv, argv, replyv})
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}
	resp := &Response{svcID, errmsg, nonRPCErr}

	return sendResponse(sWrap, resp, replyv.Interface())
}

func sendResponse(s *streamWrap, resp *Response, body interface{}) error {
	if err := s.enc.Encode(resp); err != nil {
		logger.Error("error encoding response:", err)
		s.stream.Reset()
		return err
	}
	if err := s.enc.Encode(body); err != nil {
		logger.Error("error encoding body:", err)
		s.stream.Reset()
		return err
	}
	if err := s.w.Flush(); err != nil {
		logger.Debug("error flushing response:", err)
		s.stream.Reset()
		return err
	}
	return nil
}

// Call allows a server to process a Call directly and act like a client
// to itself. This is mostly useful because LibP2P does not allow to
// create streams between a server and a client which share the same
// host. See NewClientWithServer() for more info.
func (server *Server) Call(call *Call) error {
	var err error

	sh := server.statsHandler
	if sh != nil {
		call.ctx = sh.TagRPC(call.ctx, &stats.RPCTagInfo{FullMethodName: "/" + call.SvcID.Name + "/" + call.SvcID.Method})
		beginTime := time.Now()
		begin := &stats.Begin{
			BeginTime: beginTime,
		}
		sh.HandleRPC(call.ctx, begin)

		inPayload := &stats.InPayload{
			Payload:  call,
			Length:   int(reflect.TypeOf(call.Args).Size()),
			RecvTime: beginTime,
		}
		sh.HandleRPC(call.ctx, inPayload)
		defer func() {
			end := &stats.End{
				BeginTime: beginTime,
				EndTime:   time.Now(),
			}
			if err != nil && err != io.EOF {
				end.Error = newServerError(err)
			}
			sh.HandleRPC(call.ctx, end)
		}()
	}

	var argv, replyv reflect.Value
	service, mtype, err := server.getService(call.SvcID)
	if err != nil {
		return newServerError(err)
	}

	// Use the context value from the call directly
	ctxv := reflect.ValueOf(call.ctx)

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Ptr {
		if reflect.TypeOf(call.Args).Kind() != reflect.Ptr {
			return fmt.Errorf(
				"%s.%s is being called with the wrong arg type",
				call.SvcID.Name,
				call.SvcID.Method,
			)
		}
		argv = reflect.New(mtype.ArgType.Elem())
		argv.Elem().Set(reflect.ValueOf(call.Args).Elem())
	} else {
		if reflect.TypeOf(call.Args).Kind() == reflect.Ptr {
			return fmt.Errorf(
				"%s.%s is being called with the wrong arg type",
				call.SvcID.Name,
				call.SvcID.Method,
			)
		}
		argv = reflect.New(mtype.ArgType)
		argv.Elem().Set(reflect.ValueOf(call.Args))
		argIsValue = true
	}
	// argv guaranteed to be a pointer here.
	// need dereference if the method actually takes a value.
	if argIsValue {
		argv = argv.Elem()
	}

	replyv = reflect.New(mtype.ReplyType.Elem())

	// Call service and respond
	function := mtype.method.Func

	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call(
		[]reflect.Value{
			service.rcvr,
			ctxv, // context
			argv, // argument
			replyv,
		},
	) // reply

	creplyv := reflect.ValueOf(call.Reply)
	creplyv.Elem().Set(replyv.Elem())

	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	if errInter != nil {
		return errInter.(error)
	}
	return nil
}

func (server *Server) getService(id ServiceID) (*service, *methodType, error) {
	// Look up the request.
	server.mu.RLock()
	service := server.serviceMap[id.Name]
	server.mu.RUnlock()
	if service == nil {
		err := errors.New("rpc: can't find service " + id.Name)
		return nil, nil, newServerError(err)
	}
	mtype := service.method[id.Method]
	if mtype == nil {
		err := errors.New("rpc: can't find method " + id.Method)
		return nil, nil, newServerError(err)
	}
	return service, mtype, nil
}

// All code below is provided under:
// Copyright (c) 2009 The Go Authors. All rights reserved.
// and the corresponding license. See LICENSE for more details.

// Is this an exported - upper case - name?
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//	- exported method of exported type
//	- two arguments, both of exported type
//	- the second argument is a pointer
//	- one return value, of type error
// It returns an error if the receiver is not an exported type or has
// no suitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
func (server *Server) Register(rcvr interface{}) error {
	return server.register(rcvr, "", false)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (server *Server) RegisterName(name string, rcvr interface{}) error {
	return server.register(rcvr, name, true)
}

func (server *Server) register(rcvr interface{}, name string, useName bool) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.serviceMap == nil {
		server.serviceMap = make(map[string]*service)
	}
	s := new(service)
	s.typ = reflect.TypeOf(rcvr)
	s.rcvr = reflect.ValueOf(rcvr)
	sname := reflect.Indirect(s.rcvr).Type().Name()
	if useName {
		sname = name
	}
	if sname == "" {
		s := "rpc.Register: no service name for type " + s.typ.String()
		log.Print(s)
		return errors.New(s)
	}
	if !isExported(sname) && !useName {
		s := "rpc.Register: type " + sname + " is not exported"
		log.Print(s)
		return errors.New(s)
	}
	if _, present := server.serviceMap[sname]; present {
		return errors.New("rpc: service already defined: " + sname)
	}
	s.name = sname

	// Install the methods
	s.method = suitableMethods(s.typ, true)

	if len(s.method) == 0 {
		str := ""

		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(reflect.PtrTo(s.typ), false)
		if len(method) != 0 {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type"
		}
		log.Print(str)
		return errors.New(str)
	}
	server.serviceMap[s.name] = s
	return nil
}

// suitableMethods returns suitable Rpc methods of typ, it will report
// error using log if reportErr is true.
func suitableMethods(typ reflect.Type, reportErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if method.PkgPath != "" {
			continue
		}
		// Method needs four ins: receiver, context.Context, *args, *reply.
		if mtype.NumIn() != 4 {
			if reportErr {
				log.Println("method", mname, "has wrong number of ins:", mtype.NumIn())
			}
			continue
		}

		// First argument needs to be a context
		ctxType := mtype.In(1)
		ctxIntType := reflect.TypeOf((*context.Context)(nil)).Elem()
		if !ctxType.Implements(ctxIntType) {
			if reportErr {
				log.Println(mname, "first argument is not a context.Context:", ctxType)
			}
			continue
		}

		// Second arg need not be a pointer so that's not checked.
		argType := mtype.In(2)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Println(mname, "argument type not exported:", argType)
			}
			continue
		}
		// Third arg must be a pointer.
		replyType := mtype.In(3)
		if replyType.Kind() != reflect.Ptr {
			if reportErr {
				log.Println("method", mname, "reply type not a pointer:", replyType)
			}
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			if reportErr {
				log.Println("method", mname, "reply type not exported:", replyType)
			}
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if reportErr {
				log.Println("method", mname, "has wrong number of outs:", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				log.Println("method", mname, "returns", returnType.String(), "not error")
			}
			continue
		}
		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}
