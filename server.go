// Package rpc is heavily inspired by Go standard net/rpc package. It aims to
// do the same thing, except it uses libp2p for communication and provides
// context support for cancelling operations.
//
// A server registers an object, making it visible as a service with the name
// of the type of the object. After registration, exported methods of the
// object will be accessible remotely.  A server may register multiple objects
// (services) of different types but it is an error to register multiple
// objects of the same type.
//
// Only methods that satisfy these criteria will be made available for remote
// access; other methods will be ignored:
//   - the method's type is exported.
//   - the method is exported.
//   - the method has 3 arguments.
//   - the method's first argument is a context.
//   - For normal methods:
//   - the method's second and third arguments are both exported (or builtin) types.
//   - the method's second argument is a pointer.
//   - For "streaming" methods:
//   - the method's second argument is a receiving channel (<-chan) of exported (or builtin) type.
//   - the method's third argument is a sending channel (chan<-) of exported (or builtin) type.
//   - the method has return type error.
//
// In effect, the method must look schematically like
//
//		func (t *T) MethodName(ctx context.Context, argType T1, replyType *T2) error
//	     or
//		func (t *T) MethodName(ctx context.Context, argChan <-chan T1, replyChan chan<- T2) error
//
// where T1 and T2 can be marshaled by github.com/ugorji/go/codec.
//
// In normal calls, the method's second argument represents the arguments
// provided by the caller; the third argument represents the result
// parameters to be returned to the caller. The function error response is
// passed to the client accordingly.
//
// In streaming calls, the method's second and third arguments are argument
// and replies channels. The method is expected to read from the argument
// channel until it is closed. The method is expected to send responses on the
// replies channel and close it when done. Both channels are transparently and
// asynchronously streamed on the wire between remote hosts.
//
// In order to use this package, a ready-to-go libp2p Host must be provided
// to clients and servers, along with a protocol.ID. rpc will add a stream
// handler for the given protocol.
//
// Contexts are supported and honored when provided. On the server side,
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
	"reflect"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	logging "github.com/ipfs/go-log/v2"
	stats "github.com/libp2p/go-libp2p-gorpc/stats"
)

// ContextKey is special type for using as a key with context.Context
type ContextKey string

const (
	// ContextKeyRequestSender is default key for RPC service function context to retrieve peer ID of current request sender
	ContextKeyRequestSender = ContextKey("request_sender")
	// MaxServiceIDLength specifies a maximum length for the
	// "ServiceName.MethodName" so that an attacker cannot send an
	// arbitrarily large ServiceID.
	MaxServiceIDLength = 256
)

var logger = logging.Logger("p2p-gorpc")

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

type methodType struct {
	method         reflect.Method
	ArgType        reflect.Type
	streamingArg   bool
	ReplyType      reflect.Type
	streamingReply bool
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

// String concatenates ServiceID name and method.
func (svcID ServiceID) String() string {
	return svcID.Name + "." + svcID.Method
}

// Response wraps all elements necessary to reply to a call: Service ID, Error
// and data. Responses are written to the wire in two steps. First the
// response object (without the data), then the data object.  In streaming
// calls, each reply object is prepended by a Response object, which should be
// fully empty unless there is an error.
type Response struct {
	Service ServiceID   `codec:",omitempty"`
	Error   string      `codec:",omitempty"` // error, if any.
	ErrType ErrorType   `codec:",omitempty"`
	Data    interface{} `codec:"-"` // Response data
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

// WithStreamBufferSize sets the channel buffer size for streaming requests.
func WithStreamBufferSize(size int) ServerOption {
	return func(s *Server) {
		s.streamBufferSize = size
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
	host     host.Host
	protocol protocol.ID

	statsHandler     stats.Handler
	streamBufferSize int

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
			defer stream.Close()
			s.handle(sWrap)
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

func (server *Server) handle(s *streamWrap) {
	logger.Debugf("%s: handling remote RPC from %s", server.host.ID().Pretty(), s.stream.Conn().RemotePeer())
	var svcID ServiceID
	ctx := context.Background()

	// First, read the header which tells us which service we are hoping
	// to run.  Using limDec so that a client does not potentially DDOS us
	// with a huge header for what ends up being an unauthorized method.
	err := s.dec.Decode(&svcID)
	if err != nil {
		sendError(s, ServiceID{}, newServerError(fmt.Errorf("error reading service ID: %w", err)))
	}

	// stats ----------------------
	sh := server.statsHandler
	if sh != nil {
		ctx = sh.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/" + svcID.String()})
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
	// stats end -------------------------

	logger.Debugf("RPC ServiceID is %s", svcID)

	// The service needs to have been registered with us.
	service, mtype, err := server.getService(svcID)
	if err != nil {
		sendError(s, svcID, newServerError(err))
		return
	}

	// Ensure the remote peer has permission to run  this by calling authorize().
	remotePeer := s.stream.Conn().RemotePeer()
	if server.authorize != nil && !server.authorize(remotePeer, svcID.Name, svcID.Method) {
		errMsg := fmt.Sprintf("client does not have permissions to call %s", svcID)
		sendError(s, svcID, newAuthorizationError(errors.New(errMsg)))
		return
	}
	// Right now both must be true or false, which is checked somewhere
	// else.
	if mtype.streamingArg || mtype.streamingReply {
		service.streamCall(ctx, s, mtype, svcID, server.streamBufferSize)
		return
	}

	err = service.methodCall(ctx, s, mtype, svcID)
	if err != nil {
		logger.Warning(err)
		sendError(s, svcID, err)
		return
	}
}

// Call a method by reading a single argument from the wire and return the
// response.
func (s *service) methodCall(ctx context.Context, sw *streamWrap, mtype *methodType, svcID ServiceID) error {
	ctx = context.WithValue(ctx, ContextKeyRequestSender, sw.stream.Conn().RemotePeer())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	argv, err := readArgFromStream(sw, mtype.ArgType)
	if err != nil {
		return err
	}

	// Replies are always pointers, so need Elem() to get the value.
	replyv := reflect.New(mtype.ReplyType.Elem())

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
		_, err := sw.stream.Read(p)
		if err != nil {
			cancel()
		}
	}()

	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call([]reflect.Value{s.rcvr, ctxv, argv, replyv})
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}
	resp := Response{svcID, errmsg, NonRPCErr, replyv.Interface()}

	return sendResponse(sw, resp)
}

func (s *service) streamCall(ctx context.Context, sw *streamWrap, mtype *methodType, svcID ServiceID, size int) {
	ctx = context.WithValue(ctx, ContextKeyRequestSender, sw.stream.Conn().RemotePeer())
	ctx, cancel := context.WithCancel(ctx)

	// we will need a goroutine that reads from the stream and decodes on
	// mtype.ArgTypes and sends them on channel. And we will need
	// goroutine that reads mtype.ReplyType replies, wraps themencodes them and
	// writes them to the stream.

	// Then we will need to call the things.
	argsChan := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, mtype.ArgType), size)
	repliesChan := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, mtype.ReplyType), size)
	ctxv := reflect.ValueOf(ctx)
	done := reflect.ValueOf(ctx.Done())

	var wg sync.WaitGroup
	wg.Add(2)
	// Read from stream
	go func() {
		defer wg.Done()
		// when done, close stream for reading.
		defer sw.stream.CloseRead()

		for {
			// This ensures that we don't attempt to read if the
			// context is cancelled.
			select {
			case <-ctx.Done():
				logger.Warningf("%s: reading arguments cancelled: %s", svcID, ctx.Err())
				argsChan.Close()
				return
			default:
			}

			argv, err := readArgFromStream(sw, mtype.ArgType)
			if err == io.EOF { // stream closed, no more to read.
				argsChan.Close()
				return
			} else if err != nil { // abort
				logger.Warning(err)
				cancel() // signal everyone to abort.
				argsChan.Close()
				return
			}

			// This ensures that we don't deadlock on send when
			// there is no one receiving if the context is
			// cancelled.
			chosen, _, _ := reflect.Select([]reflect.SelectCase{
				{
					Dir:  reflect.SelectRecv,
					Chan: done, // chan done
				},
				{
					Dir:  reflect.SelectSend,
					Chan: argsChan,
					Send: argv,
				},
			})
			if chosen == 0 {
				logger.Warningf("%s: reading arguments cancelled: %s", svcID, ctx.Err())
				argsChan.Close()
				return
			}
		}
	}()

	// Read from replies
	go func() {
		defer wg.Done()

		for {
			// This ensures that we don't deadlock on reading
			// replies if they are not sent and the context is
			// cancelled.
			chosen, v, ok := reflect.Select([]reflect.SelectCase{
				{
					Dir:  reflect.SelectRecv,
					Chan: done, // chan done
				},
				{
					Dir:  reflect.SelectRecv,
					Chan: repliesChan,
				},
			})

			if chosen == 0 { // context cancelled
				logger.Warningf("%s: reading replies cancelled: %s", svcID, ctx.Err())
				go drainChannel(repliesChan)
				return
			}

			if !ok { // repliesChanClosed
				logger.Debugf("%s: reply channel closed", svcID)
				return
			}

			// This ensures that we don't attempt to send a reply
			// on the wire if the context is cancelled.
			select {
			case <-ctx.Done():
				logger.Warningf("%s: reading replies cancelled: %s", svcID, ctx.Err())
				go drainChannel(repliesChan)
				return
			default:
			}

			err := sendResponse(sw, Response{
				Service: svcID,
				Data:    v.Interface(),
			})
			if err != nil {
				logger.Warning(err)
				cancel() // signal everyone to abort
				go drainChannel(repliesChan)
				return
			}
		}
	}()

	// Finally, we need to call the actual function
	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.  This
	// hangs. The function will do its job. The only way we can signal
	// that it should stop doings its job is by cancelling ctxv or by closing argsChan.
	returnValues := function.Call([]reflect.Value{s.rcvr, ctxv, argsChan, repliesChan})
	errInter := returnValues[0].Interface()
	if errInter != nil {
		cancel()
		wg.Wait()
		err := errInter.(error)
		logger.Debugf("%s: attempt to send error: %s", svcID, err)
		sendError(sw, svcID, err)
		sw.stream.Close()
		return
	}

	logger.Debugf("%s function call finished cleanly", svcID)

	// This means everything went well, OR an error happened with the
	// stream, the receiving or sending goroutines Reseted it, closed the
	// arguments channel and the function exited cleanly. In this case the
	// sending site may have received a stream reset at least, and we
	// should have logged something.
	wg.Wait() // wait for things to exist cleanly if they haven't
	sw.stream.Close()
	cancel()
}

func readArgFromStream(sw *streamWrap, argType reflect.Type) (reflect.Value, error) {
	var argv reflect.Value
	argIsValue := false

	if argType.Kind() == reflect.Ptr {
		argv = reflect.New(argType.Elem())
	} else {
		argv = reflect.New(argType) // pointer to ArgType
		argIsValue = true
	}
	// argv guaranteed to be a pointer now, so we can decode on top.
	if err := sw.dec.Decode(argv.Interface()); err != nil {
		if err == io.EOF {
			return reflect.ValueOf(nil), err
		}
		return argv, newServerError(err)
	}
	if argIsValue {
		argv = argv.Elem()
	}
	return argv, nil
}

// sendResponse sends a Response by first serializing the Response object and
// then serializing the Response.Data object directly.
func sendResponse(s *streamWrap, resp Response) error {
	if err := s.enc.Encode(resp); err != nil {
		s.stream.Reset()
		return fmt.Errorf("error encoding response: %w", err)
	}

	if err := s.enc.Encode(resp.Data); err != nil {
		s.stream.Reset()
		return fmt.Errorf("error encoding body: %w", err)
	}

	if err := s.w.Flush(); err != nil {
		s.stream.Reset()
		return fmt.Errorf("error flushing response/body: %w", err)
	}
	return nil
}

func sendError(s *streamWrap, svcID ServiceID, err error) error {
	return sendResponse(s, Response{
		Service: svcID,
		Error:   err.Error(),
		ErrType: responseErrorType(err),
		Data:    nil,
	})
}

// Call allows a server to process a Call directly and act like a client
// to itself. This is mostly useful because libp2p does not allow to
// create streams between a server and a client which share the same
// host. See NewClientWithServer() for more info.
func (server *Server) serverCall(call *Call) error {
	var err error

	// metrics ---------------------------------------
	sh := server.statsHandler
	if sh != nil {
		call.ctx = sh.TagRPC(call.ctx, &stats.RPCTagInfo{FullMethodName: "/" + call.SvcID.String()})
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
	// metrics -----------------------------------------------

	var argv, replyv reflect.Value
	service, mtype, err := server.getService(call.SvcID)
	if err != nil {
		return newServerError(err)
	}

	ctx := context.WithValue(call.ctx, ContextKeyRequestSender, server.ID()) // add local peer id as request sender

	// Use the context value from the call directly
	ctxv := reflect.ValueOf(ctx)

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Ptr {
		if reflect.TypeOf(call.Args).Kind() != reflect.Ptr {
			return fmt.Errorf("%s is being called with the wrong arg type", call.SvcID)
		}
		argv = reflect.New(mtype.ArgType.Elem())
		argv.Elem().Set(reflect.ValueOf(call.Args).Elem())
	} else {
		if reflect.TypeOf(call.Args).Kind() == reflect.Ptr {
			return fmt.Errorf("%s is being called with the wrong arg type", call.SvcID)
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

// Stream allows a server to process a streaming Call directly and act like a
// client to itself. This is mostly useful because libp2p does not allow to
// create streams between a server and a client which share the same host. See
// NewClientWithServer() for more info.
func (server *Server) serverStream(call *Call) error {
	service, mtype, err := server.getService(call.SvcID)
	if err != nil {
		return newServerError(err)
	}

	ctx := context.WithValue(call.ctx, ContextKeyRequestSender, server.ID()) // add local peer id as request sender

	if !mtype.streamingArg || !mtype.streamingReply {
		return fmt.Errorf("%s is not a streaming method", call.SvcID)
	}

	if call.StreamArgs.Type().Elem() != mtype.ArgType {
		return fmt.Errorf("%s send channel is of wrong type", call.SvcID)
	}

	if call.StreamReplies.Type().Elem() != mtype.ReplyType {
		return fmt.Errorf("%s receive channel is of wrong type", call.SvcID)
	}

	// This is easier than streaming as we can just plug the calls arguments
	// into the function and let it deal with it.
	ctxv := reflect.ValueOf(ctx)

	function := mtype.method.Func
	returnValues := function.Call([]reflect.Value{service.rcvr, ctxv, call.StreamArgs, call.StreamReplies})

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

func drainChannel(ch reflect.Value) {
	for {
		if _, ok := ch.Recv(); !ok {
			return
		}
	}
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
//   - exported method of exported type
//   - context as first argument
//   - input as second argument: exported type or channel of exported type
//   - output as third argument: a pointer to a exported type, or a channel of exported type
//   - one return value, of type error.
//
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
		logger.Error(s)
		return errors.New(s)
	}
	if !isExported(sname) && !useName {
		s := "rpc.Register: type " + sname + " is not exported"
		logger.Error(s)
		return errors.New(s)
	}
	if _, present := server.serviceMap[sname]; present {
		return errors.New("rpc: service already defined: " + sname)
	}
	s.name = sname

	// Install the methods
	s.method = suitableMethods(s.name, s.typ, true)

	if len(s.method) == 0 {
		str := ""

		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(s.name, reflect.PtrTo(s.typ), false)
		if len(method) != 0 {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type"
		}
		logger.Error(str)
		return errors.New(str)
	}
	server.serviceMap[s.name] = s
	return nil
}

// suitableMethods returns suitable Rpc methods of typ, it will report
// error using log if reportErr is true.
func suitableMethods(sname string, typ reflect.Type, reportErr bool) map[string]*methodType {
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
				logger.Error("method ", mname, " has wrong number of ins: ", mtype.NumIn())
			}
			continue
		}

		// First argument needs to be a context
		ctxType := mtype.In(1)
		ctxIntType := reflect.TypeOf((*context.Context)(nil)).Elem()
		if !ctxType.Implements(ctxIntType) {
			if reportErr {
				logger.Error(mname, "first argument is not a context.Context: ", ctxType)
			}
			continue
		}

		// Second arg must be exported or a channel with an exported type.
		argType := mtype.In(2)
		argTypeKind := argType.Kind()
		streamingArg := false

		switch argTypeKind {
		case reflect.Chan:
			aElem := argType.Elem()
			if !isExportedOrBuiltinType(aElem) {
				if reportErr {
					logger.Error(mname, " argument channel type not exported: ", aElem)
				}
				continue
			}
			if argType.ChanDir() != reflect.RecvDir {
				if reportErr {
					logger.Error("method ", mname, " argument channel is not a receive-only channel")
				}
				continue
			}
			argType = aElem
			streamingArg = true
		default:
			if !isExportedOrBuiltinType(argType) {
				if reportErr {
					logger.Error(mname, " argument type not exported: ", argType)
				}
				continue
			}
		}
		// Third arg must be a pointer or a channel with an exported type.
		replyType := mtype.In(3)
		replyTypeKind := replyType.Kind()
		streamingReply := false
		switch replyTypeKind {
		case reflect.Chan:
			rElem := replyType.Elem()
			if !isExportedOrBuiltinType(rElem) {
				if reportErr {
					logger.Error("method ", mname, " reply channel type not exported: ", rElem)
				}
				continue
			}
			if replyType.ChanDir() != reflect.SendDir {
				if reportErr {
					logger.Error("method ", mname, " reply channel is not a send-only channel")
				}
				continue
			}
			replyType = rElem
			streamingReply = true
		case reflect.Ptr:
			if !isExportedOrBuiltinType(replyType) {
				if reportErr {
					logger.Error("method ", mname, " reply type not exported: ", replyType)
				}
				continue
			}
		default:
			if reportErr {
				logger.Error("method ", mname, " reply type not a pointer or channel: ", replyType)
			}
			continue
		}

		// If one argument is a streaming argument but not both, then complain
		if (streamingArg || streamingReply) && streamingArg != streamingReply {
			if reportErr {
				logger.Error("method ", mname, " argument and reply must both be channels")
			}
			continue
		}

		// Method needs one out.
		if mtype.NumOut() != 1 {
			if reportErr {
				logger.Error("method ", mname, " has wrong number of outs: ", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				logger.Error("method ", mname, " returns ", returnType.String(), ", not error")
			}
			continue
		}

		if len(sname)+len(mname) > MaxServiceIDLength {
			if reportErr {
				logger.Error("The service ID ", sname, ".", mname, " is longer than allowed limit: ", MaxServiceIDLength)
			}
			continue
		}
		methods[mname] = &methodType{
			method:         method,
			ArgType:        argType,
			streamingArg:   streamingArg,
			ReplyType:      replyType,
			streamingReply: streamingReply,
		}
	}
	return methods
}

// GetRequestSender gets current request sender from RPC service's function context
func GetRequestSender(ctx context.Context) (peer.ID, error) {
	v := ctx.Value(ContextKeyRequestSender)
	if v == nil {
		return "", fmt.Errorf("sender is missing")
	}

	p, ok := v.(peer.ID)
	if !ok {
		return "", fmt.Errorf("cannot convert request_sender context value to peer.ID")
	}

	if err := p.Validate(); err != nil {
		return "", err
	}

	return p, nil
}
