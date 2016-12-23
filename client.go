package rpc

import (
	"context"
	"errors"

	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

// Call represents an active RPC.
type Call struct {
	Dest  peer.ID
	SvcID ServiceID   // The name of the service and method to call.
	Args  interface{} // The argument to the function (*struct).
	Reply interface{} // The reply from the function (*struct).
	Error error       // After completion, the error status.
	Done  chan *Call  // Strobes when call is complete.
}

type Client struct {
	host     host.Host
	protocol protocol.ID
	server   *Server
}

func NewClient(h host.Host, p protocol.ID) *Client {
	return &Client{
		host:     h,
		protocol: p,
	}
}

func NewClientWithServer(h host.Host, p protocol.ID, s *Server) *Client {
	c := NewClient(h, p)
	c.server = s
	return c
}

func (c *Client) Call(dest peer.ID, svcName string, svcMethod string, args interface{}, reply interface{}) error {
	call := &Call{
		Dest:  dest,
		SvcID: ServiceID{svcName, svcMethod},
		Args:  args,
		Reply: reply,
		Error: nil,
		Done:  make(chan *Call, 1),
	}
	c.makeCall(call)
	call = <-call.Done
	return call.Error
}

func (c *Client) Go(dest peer.ID, svcName string, svcMethod string, args interface{}, reply interface{}) error {
	call := &Call{
		Dest:  dest,
		SvcID: ServiceID{svcName, svcMethod},
		Args:  args,
		Reply: reply,
		Error: nil,
		Done:  make(chan *Call, 0),
	}
	c.makeCall(call)
	return call.Error
}

func (c *Client) makeCall(call *Call) {
	logger.Debug("make call")
	if c.host == nil {
		panic("no host set")
	}
	if c.protocol == "" {
		panic("no protocol set")
	}

	if call.Dest == c.host.ID() {
		logger.Debug("making local call")
		if c.server == nil {
			err := errors.New("Cannot make local calls: server not set")
			call.Error = err
			call.done()
			logger.Error(err)
			return
		}
		c.server.Call(call)
		call.done()
		return
	}
	c.send(call)
}

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

func (call *Call) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		logger.Debugf("discarding %s.%s call reply",
			call.SvcID.Name, call.SvcID.Method)
	}
}
