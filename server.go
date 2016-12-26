/*
Package rpc is heavily inspired by Go standard net/rpc package. It aims to do
the same function, except it uses Libp2p for communication.

A server registers an object, making it visible as a service with the name of
the type of the object.  After registration, exported methods of the object
will be accessible remotely.  A server may register multiple objects
(services) of different types but it is an error to register multiple objects
of the same type.

Only methods that satisfy these criteria will be made available for remote
access; other methods will be ignored:
	- the method's type is exported.
	- the method is exported.
	- the method has two arguments, both exported (or builtin) types.
	- the method's second argument is a pointer.
	- the method has return type error.

In effect, the method must look schematically like

	func (t *T) MethodName(argType T1, replyType *T2) error

where T1 and T2 can be marshaled by encoding/gob.

The method's first argument represents the arguments provided by the caller;
the second argument represents the result parameters to be returned to the
caller.  The method's return value, if non-nil, is passed back as a string
that the client sees as if created by errors.New.  If an error is returned,
the reply parameter will not be sent back to the client.

In order to use this package, a ready-to-go LibP2P Host must be provided
to clients and servers, along with a protocol.ID. rpc will add a stream
handler for the given protocol. Hosts must be ready to speak to clients,
that is, peers must be part of the peerstore along with keys if secio
communication is required.
*/
package rpc

import (
	"errors"
	"log"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"

	peer "gx/ipfs/QmfMmLGoKzCHDN7cGgk64PJr4iipzidDRME8HABSJqvmhC/go-libp2p-peer"

	host "gx/ipfs/QmPTGbC34bPKaUm9wTxBo7zSCac7pDuG42ZmnXC718CKZZ/go-libp2p-host"
	inet "gx/ipfs/QmQx1dHDDYENugYgqA22BaBrRfuv1coSsuPiM7rYh1wwGH/go-libp2p-net"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	protocol "gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
)

var logger = logging.Logger("libp2p-rpc")

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

	mu         sync.RWMutex // protects the serviceMap
	serviceMap map[string]*service
}

// NewServer creates a Server object with the given LibP2P host
// and protocol.
func NewServer(h host.Host, p protocol.ID) *Server {
	s := &Server{
		host:     h,
		protocol: p,
	}

	h.SetStreamHandler(p, func(stream inet.Stream) {
		sWrap := wrapStream(stream)
		defer stream.Close()
		err := s.handle(sWrap)
		if err != nil {
			logger.Error("error handling RPC:", err)
			resp := &Response{ServiceID{}, err.Error()}
			sendResponse(sWrap, resp, nil)
		}
	})
	return s
}

// ID returns the peer.ID of the host associated with this server.
func (server *Server) ID() peer.ID {
	if server.host == nil {
		panic("server has no host")
	}
	return server.host.ID()
}

func (server *Server) handle(s *streamWrap) error {
	logger.Debugf("%s: handling remote RPC", server.host.ID().Pretty())
	var svcID ServiceID
	var argv, replyv reflect.Value

	err := s.dec.Decode(&svcID)
	if err != nil {
		return err
	}

	logger.Debugf("RPC ServiceID is %s.%s", svcID.Name, svcID.Method)

	service, mtype, err := server.getService(svcID)
	if err != nil {
		return err
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
		return err
	}
	if argIsValue {
		argv = argv.Elem()
	}

	replyv = reflect.New(mtype.ReplyType.Elem())

	// Call service and respond
	return service.svcCall(s, mtype, svcID, argv, replyv)
}

// svcCall calls the actual method associated
func (s *service) svcCall(sWrap *streamWrap, mtype *methodType, svcID ServiceID, argv, replyv reflect.Value) error {
	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call([]reflect.Value{s.rcvr, argv, replyv})
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}
	resp := &Response{svcID, errmsg}

	return sendResponse(sWrap, resp, replyv.Interface())
}

func sendResponse(s *streamWrap, resp *Response, body interface{}) error {
	if err := s.enc.Encode(resp); err != nil {
		logger.Error("error encoding response:", err)
		return err
	}
	if err := s.enc.Encode(body); err != nil {
		logger.Error("error encoding body:", err)
		return err
	}
	if err := s.w.Flush(); err != nil {
		logger.Debug("error flushing response:", err)
		return err
	}
	return nil
}

// Call allows a server to process a Call directly and act like a client
// to itself. This is mostly useful because LibP2P does not allow to
// create streams between a server and a client which share the same
// host. See NewClientWithServer() for more info.
func (server *Server) Call(call *Call) error {
	var argv, replyv reflect.Value
	service, mtype, err := server.getService(call.SvcID)
	if err != nil {
		return err
	}

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mtype.ArgType.Elem())
		argv.Elem().Set(reflect.ValueOf(call.Args).Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argv.Elem().Set(reflect.ValueOf(call.Args))
		argIsValue = true
	}
	if argIsValue {
		argv = argv.Elem()
	}

	replyv = reflect.New(mtype.ReplyType.Elem())

	// Call service and respond
	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call([]reflect.Value{
		service.rcvr,
		argv,
		replyv})
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	if errInter != nil {
		return err
	}
	creplyv := reflect.ValueOf(call.Reply)
	creplyv.Elem().Set(replyv.Elem())
	return nil
}

func (server *Server) getService(id ServiceID) (*service, *methodType, error) {
	// Look up the request.
	server.mu.RLock()
	service := server.serviceMap[id.Name]
	server.mu.RUnlock()
	if service == nil {
		err := errors.New("rpc: can't find service " + id.Name)
		return nil, nil, err
	}
	mtype := service.method[id.Method]
	if mtype == nil {
		err := errors.New("rpc: can't find method " + id.Method)
		return nil, nil, err
	}
	return service, mtype, nil
}

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
		// Method needs three ins: receiver, *args, *reply.
		if mtype.NumIn() != 3 {
			if reportErr {
				log.Println("method", mname, "has wrong number of ins:", mtype.NumIn())
			}
			continue
		}
		// First arg need not be a pointer.
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Println(mname, "argument type not exported:", argType)
			}
			continue
		}
		// Second arg must be a pointer.
		replyType := mtype.In(2)
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
