package rpc

import (
	"context"
	"errors"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

// Call represents an active RPC. Calls are used to indicate completion
// of RPC requests and are returned within the provided channel in
// the Go() and Call() functions.
type Call struct {
	Dest  peer.ID
	SvcID ServiceID   // The name of the service and method to call.
	Args  interface{} // The argument to the function (*struct).
	Reply interface{} // The reply from the function (*struct).
	Error error       // After completion, the error status.
	Done  chan *Call  // Strobes when call is complete.
}

// Client represents an RPC client which can perform calls to a remote
// (or local, see below) Server.
type Client struct {
	host     host.Host
	protocol protocol.ID
	server   *Server
}

// NewClient returns a new Client which uses the given LibP2P host
// and protocol ID, which must match the one used by the server.
// The Host must be correctly configured to be able to open streams
// to the server (addresses and keys in Peerstore etc.).
//
// The client returned will not be able to run any local requests
// if the Server is sharing the same LibP2P host. See NewClientWithServer
// if this is a usecase.
func NewClient(h host.Host, p protocol.ID) *Client {
	return &Client{
		host:     h,
		protocol: p,
	}
}

// NewClientWithServer takes an additional RPC Server and returns a Client
// which will perform any requests to itself by using the given Server.Call()
// directly. It is assumed that Client and Server share the same LibP2P host.
func NewClientWithServer(h host.Host, p protocol.ID, s *Server) *Client {
	c := NewClient(h, p)
	c.server = s
	return c
}

// ID returns the peer.ID of the host associated with this client.
func (c *Client) ID() peer.ID {
	if c.host == nil {
		return ""
	}
	return c.host.ID()
}

// Call performs an RPC call to a registered Server service and blocks until
// completed. If dest is empty ("") or matches the Client's host ID, it will
// attempt to use the local configured Server when possible.
func (c *Client) Call(dest peer.ID, svcName string, svcMethod string, args interface{}, reply interface{}) error {
	done := make(chan *Call, 1)
	c.Go(dest, svcName, svcMethod, args, reply, done)
	call := <-done
	return call.Error
}

// Go performs an RPC call asynchronously. The associated Call will be placed
// in the provided channel upon completion, holding any Reply or Errors.
//
// The provided done channel must be nil, or have capacity for 1 element
// at least, or a panic will be triggered.
//
// If dest is empty ("") or matches the Client's host ID, it will
// attempt to use the local configured Server when possible.
func (c *Client) Go(dest peer.ID, svcName string, svcMethod string, args interface{}, reply interface{}, done chan *Call) error {
	if done == nil {
		done = make(chan *Call, 1)
	} else {
		if cap(done) == 0 {
			panic("done channel has no capacity")
		}
	}
	call := &Call{
		Dest:  dest,
		SvcID: ServiceID{svcName, svcMethod},
		Args:  args,
		Reply: reply,
		Error: nil,
		Done:  done,
	}
	go c.makeCall(call)
	return nil
}

// makeCall decides if a call can be performed. If it's a local
// call it will use the configured server if set.
func (c *Client) makeCall(call *Call) {
	logger.Debugf("makeCall: %s.%s",
		call.SvcID.Name,
		call.SvcID.Method)

	// Handle local RPC calls
	if call.Dest == "" || call.Dest == c.host.ID() {
		logger.Debugf("local call: %s.%s",
			call.SvcID.Name, call.SvcID.Method)
		if c.server == nil {
			err := errors.New(
				"Cannot make local calls: server not set")
			logger.Error(err)
			call.Error = err
			call.done()
			return
		}
		err := c.server.Call(call)
		call.Error = err
		if err != nil {
			logger.Error(err)
		}
		call.done()
		return
	}

	// Handle remote RPC calls
	if c.host == nil {
		panic("no host set: cannot perform remote call")
	}
	if c.protocol == "" {
		panic("no protocol set: cannot perform remote call")
	}
	c.send(call)
}

// send makes a REMOTE RPC call by initiating a libP2P stream to the
// destination and waiting for a response.
func (c *Client) send(call *Call) {
	logger.Debug("sending remote call")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := c.host.NewStream(ctx, call.Dest, c.protocol)
	if err != nil {
		call.Error = err
		call.Done <- call
		return
	}
	defer s.Close()
	sWrap := wrapStream(s)

	logger.Debugf("sending RPC %s.%s to %s", call.SvcID.Name,
		call.SvcID.Method, call.Dest)
	if err := sWrap.enc.Encode(call.SvcID); err != nil {
		call.Error = err
		call.done()
		return
	}
	if err := sWrap.enc.Encode(call.Args); err != nil {
		call.Error = err
		call.done()
		return
	}

	if err := sWrap.w.Flush(); err != nil {
		call.Error = err
		call.done()
		return
	}
	receiveResponse(sWrap, call)
}

// receiveResponse reads a response to an RPC call
func receiveResponse(s *streamWrap, call *Call) {
	logger.Debugf("waiting response for %s.%s to %s", call.SvcID.Name,
		call.SvcID.Method, call.Dest)
	var resp Response
	defer call.done()
	if err := s.dec.Decode(&resp); err != nil {
		call.Error = err
		return
	}

	if e := resp.Error; e != "" {
		call.Error = errors.New(e)
		return
	}

	if err := s.dec.Decode(call.Reply); err != nil {
		call.Error = err
		return
	}
}

// done places the completed call in the done channel.
func (call *Call) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		logger.Debugf("discarding %s.%s call reply",
			call.SvcID.Name, call.SvcID.Method)
	}
}
