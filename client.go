package rpc

import (
	"context"
	"errors"
	"io"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

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
func (c *Client) Call(dest peer.ID, svcName string, svcMethod string, args, reply interface{}) error {
	ctx := context.Background()
	return c.CallWithContext(ctx, dest, svcName, svcMethod, args, reply)
}

// CallWithContext performs a Call() with a user provided context. This gives
// the user the possibility of cancelling the operation at any point.
func (c *Client) CallWithContext(ctx context.Context, dest peer.ID, svcName, svcMethod string, args, reply interface{}) error {
	done := make(chan *Call, 1)
	call := newCall(ctx, dest, svcName, svcMethod, args, reply, done)
	go c.makeCall(call)
	<-done
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
func (c *Client) Go(dest peer.ID, svcName, svcMethod string, args, reply interface{}, done chan *Call) error {
	ctx := context.Background()
	return c.GoWithContext(ctx, dest, svcName, svcMethod, args, reply, done)
}

// GoWithContext performs a Go() call with the provided context, allowing
// the user to cancel the operation.
func (c *Client) GoWithContext(ctx context.Context, dest peer.ID, svcName, svcMethod string, args, reply interface{}, done chan *Call) error {
	if done == nil {
		done = make(chan *Call, 1)
	} else {
		if cap(done) == 0 {
			panic("done channel has no capacity")
		}
	}
	call := newCall(ctx, dest, svcName, svcMethod, args, reply, done)
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
			call.doneWithError(err)
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

	s, err := c.host.NewStream(call.ctx, call.Dest, c.protocol)
	if err != nil {
		call.doneWithError(err)
		return
	}
	defer s.Close()
	go call.watchContextWithStream(s)

	sWrap := wrapStream(s)

	logger.Debugf("sending RPC %s.%s to %s", call.SvcID.Name,
		call.SvcID.Method, call.Dest)
	if err := sWrap.enc.Encode(call.SvcID); err != nil {
		call.doneWithError(err)
		s.Reset()
		return
	}
	if err := sWrap.enc.Encode(call.Args); err != nil {
		call.doneWithError(err)
		s.Reset()
		return
	}

	if err := sWrap.w.Flush(); err != nil {
		call.doneWithError(err)
		s.Reset()
		return
	}
	receiveResponse(sWrap, call)
}

// receiveResponse reads a response to an RPC call
func receiveResponse(s *streamWrap, call *Call) {
	logger.Debugf("waiting response for %s.%s to %s", call.SvcID.Name,
		call.SvcID.Method, call.Dest)
	var resp Response
	if err := s.dec.Decode(&resp); err != nil {
		call.doneWithError(err)
		s.stream.Reset()
		return
	}

	defer call.done()
	if e := resp.Error; e != "" {
		call.Error = errors.New(e)
	}

	// Even on error we sent the reply so it needs to be
	// read
	if err := s.dec.Decode(call.Reply); err != nil && err != io.EOF {
		call.Error = err
	}
	return
}
