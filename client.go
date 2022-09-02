package rpc

import (
	"context"
	"io"
	"reflect"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	stats "github.com/libp2p/go-libp2p-gorpc/stats"
)

// ClientOption allows for functional setting of options on a Client.
type ClientOption func(*Client)

// WithClientStatsHandler provides an implementation of stats.Handler to be
// used by the Client.
func WithClientStatsHandler(h stats.Handler) ClientOption {
	return func(c *Client) {
		c.statsHandler = h
	}
}

// WithMultiStreamBufferSize sets the channel sizes for multiStream calls.
// Reading from the argument channel will proceed as long as none of the
// destinations have filled their buffer. See MultiStream().
func WithMultiStreamBufferSize(size int) ClientOption {
	return func(c *Client) {
		c.multiStreamBufferSize = size
	}
}

// Client represents an RPC client which can perform calls to a remote
// (or local, see below) Server.
type Client struct {
	host                  host.Host
	protocol              protocol.ID
	server                *Server
	statsHandler          stats.Handler
	multiStreamBufferSize int
}

// NewClient returns a new Client which uses the given libp2p host
// and protocol ID, which must match the one used by the server.
// The Host must be correctly configured to be able to open streams
// to the server (addresses and keys in Peerstore etc.).
//
// The client returned will not be able to run any "local" requests (to its
// own peer ID) if a server is configured with the same Host becase libp2p
// hosts cannot open streams to themselves. For this, pass the server directly
// using NewClientWithServer.
func NewClient(h host.Host, p protocol.ID, opts ...ClientOption) *Client {
	c := &Client{
		host:     h,
		protocol: p,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// NewClientWithServer takes an additional RPC Server and returns a Client.
//
// Unlike the normal client, this one will be able to perform any requests to
// itself by using the given directly (and way more efficiently). It is
// assumed that Client and Server share the same libp2p host in this case.
func NewClientWithServer(h host.Host, p protocol.ID, s *Server, opts ...ClientOption) *Client {
	c := NewClient(h, p, opts...)
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
// completed, returning any errors.
//
// The args parameter represents the service's method args and must be of
// exported or builtin type. The reply type will be used to provide a response
// and must be a pointer to an exported or builtin type. Otherwise a panic
// will occurr.
//
// If dest is empty ("") or matches the Client's host ID, it will
// attempt to use the local configured Server when possible.
func (c *Client) Call(
	dest peer.ID,
	svcName, svcMethod string,
	args, reply interface{},
) error {
	ctx := context.Background()
	return c.CallContext(ctx, dest, svcName, svcMethod, args, reply)
}

// CallContext performs an RPC call to a registered Server service and blocks
// until completed, returning any errors. It takes a context which can be used
// to abort the call at any point.
//
// The args parameter represents the service's method args and must be of
// exported or builtin type. The reply type will be used to provide a response
// and must be a pointer to an exported or builtin type. Otherwise a panic
// will occurr.
//
// If dest is empty ("") or matches the Client's host ID, it will
// attempt to use the local configured Server when possible.
func (c *Client) CallContext(
	ctx context.Context,
	dest peer.ID,
	svcName, svcMethod string,
	args, reply interface{},
) error {
	done := make(chan *Call, 1)
	call := newCall(ctx, dest, svcName, svcMethod, args, reply, done)
	go c.makeCall(call)
	<-done

	return call.getError()
}

// Go performs an RPC call asynchronously. The associated Call will be placed
// in the provided channel upon completion, holding any Reply or Errors.
//
// The args parameter represents the service's method args and must be of
// exported or builtin type. The reply type will be used to provide a response
// and must be a pointer to an exported or builtin type. Otherwise a panic
// will occurr.
//
// The provided done channel must be nil, or have capacity for 1 element
// at least, or a panic will be triggered.
//
// If dest is empty ("") or matches the Client's host ID, it will
// attempt to use the local configured Server when possible.
func (c *Client) Go(
	dest peer.ID,
	svcName, svcMethod string,
	args, reply interface{},
	done chan *Call,
) error {
	ctx := context.Background()
	return c.GoContext(ctx, dest, svcName, svcMethod, args, reply, done)
}

// GoContext performs an RPC call asynchronously. The provided context can be
// used to cancel the operation. The associated Call will be placed in the
// provided channel upon completion, holding any Reply or Errors.
//
// The args parameter represents the service's method args and must be of
// exported or builtin type. The reply type will be used to provide a response
// and must be a pointer to an exported or builtin type. Otherwise a panic
// will occurr.
//
// The provided done channel must be nil, or have capacity for 1 element
// at least, or a panic will be triggered.
//
// If dest is empty ("") or matches the Client's host ID, it will
// attempt to use the local configured Server when possible.
func (c *Client) GoContext(
	ctx context.Context,
	dest peer.ID,
	svcName, svcMethod string,
	args, reply interface{},
	done chan *Call,
) error {
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

// MultiCall performs a CallContext() to multiple destinations, using the same
// service name, method and arguments. It will not return until all calls have
// done so. The contexts, destinations and replies must match in length and
// will be used in order (ctxs[i] is used for dests[i] which obtains
// replies[i] and error[i]).
//
// The calls will be triggered in parallel (with one goroutine for each).
func (c *Client) MultiCall(
	ctxs []context.Context,
	dests []peer.ID,
	svcName, svcMethod string,
	args interface{},
	replies []interface{},
) []error {

	ok := checkMatchingLengths(
		len(ctxs),
		len(dests),
		len(replies),
	)

	if !ok {
		panic("ctxs, dests and replies must match in length")
	}

	var wg sync.WaitGroup
	errs := make([]error, len(dests))

	for i := range dests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.CallContext(
				ctxs[i],
				dests[i],
				svcName,
				svcMethod,
				args,
				replies[i])
			errs[i] = err
		}(i)
	}
	wg.Wait()
	return errs
}

// MultiGo performs a GoContext() call to multiple destinations, using the same
// service name, method and arguments. MultiGo will return as right after
// performing all the calls. See the Go() documentation for more information.
//
// The provided done channels must be nil, or have capacity for 1 element
// at least, or a panic will be triggered.
//
// The contexts, destinations, replies and done channels  must match in length
// and will be used in order (ctxs[i] is used for dests[i] which obtains
// replies[i] with dones[i] signalled upon completion).
func (c *Client) MultiGo(
	ctxs []context.Context,
	dests []peer.ID,
	svcName, svcMethod string,
	args interface{},
	replies []interface{},
	dones []chan *Call,
) error {

	ok := checkMatchingLengths(
		len(ctxs),
		len(dests),
		len(replies),
		len(dones),
	)
	if !ok {
		panic("ctxs, dests, replies and dones must match in length")
	}

	for i := range ctxs {
		c.GoContext(
			ctxs[i],
			dests[i],
			svcName,
			svcMethod,
			args,
			replies[i],
			dones[i],
		)
	}

	return nil
}

// Stream performs a streaming RPC call. It receives two arguments which both
// must be channels of exported or builtin types. The first is a channel from
// which objects are read and sent on the wire. The second is a channel to
// receive the replies from. Calling with the wrong types will cause a panic.
//
// The sending channel should be closed by the caller for successful
// completion of the call. The replies channel is closed by us when there is
// nothing else to receive (call finished or an error happened). The sending
// channel is drained in the background in case of error, so it is recommended
// that senders diligently close when an error happens to be able to free
// resources.
//
// The function only returns when the operation is finished successful (both
// channels are closed) or when an error has occurred.
func (c *Client) Stream(
	ctx context.Context,
	dest peer.ID,
	svcName, svcMethod string,
	argsChan interface{},
	repliesChan interface{},
) error {
	vArgsChan := reflect.ValueOf(argsChan)
	vRepliesChan := reflect.ValueOf(repliesChan)

	done := make(chan *Call, 1)
	call := newStreamingCall(ctx, dest, svcName, svcMethod, vArgsChan, vRepliesChan, done)
	go c.makeStream(call)
	<-done
	return call.getError()
}

// MultiStream performs parallel Stream() calls to multiple peers using a
// single arguments channel for arguments and a single replies channel that
// aggregates all replies. Errors from each destination are provided in the
// response. Channel types should be exported or builtin, otherwise a panic
// will be triggered.
//
// In order to replicate the argsChan values to several destinations and sed
// the replies into a single channel, intermediary channels for each call are
// created. These channels are buffered per the WithMultiStreamBufferSize()
// option. If the buffers is exausted for one of the sending or the receiving
// channels, the sending or receiving stalls. Therefore it is recommended to
// have enough buffering to allow that slower destinations do not delay
// everyone else.
func (c *Client) MultiStream(
	ctx context.Context,
	dests []peer.ID,
	svcName, svcMethod string,
	argsChan interface{},
	repliesChan interface{},
) []error {
	n := len(dests)
	sID := ServiceID{svcName, svcMethod}

	vArgsChan := reflect.ValueOf(argsChan)
	vRepliesChan := reflect.ValueOf(repliesChan)
	argsChanType := reflect.TypeOf(argsChan)
	repliesChanType := reflect.TypeOf(repliesChan)

	checkChanTypesValid(sID, vArgsChan, reflect.RecvDir)
	checkChanTypesValid(sID, vRepliesChan, reflect.SendDir)

	// Make slices of N channels of the same type as the argsChan and
	// repliesChan. We will use them for every Stream() call. They are
	// buffered per multiStreamBufferSize.
	vArgsChannels := makeChanSliceOf(argsChanType, n, c.multiStreamBufferSize)
	vRepliesChannels := makeChanSliceOf(repliesChanType, n, c.multiStreamBufferSize)

	// Make slices of contexts and cancels. We will use them to cancel
	// sending to channels when a Stream() call has failed.
	teeCtxs := make([]context.Context, n)
	teeCancels := make([]context.CancelFunc, n)
	for i := 0; i < n; i++ {
		teeCtxs[i], teeCancels[i] = context.WithCancel(ctx)
	}

	// To hold responses.
	errs := make([]error, n)

	var wg sync.WaitGroup

	// First, launch N stream calls to every destination using the
	// channels we created and everything else provided by the caller.
	// Collect errors in errs, and if they happen, cancel associated
	// context.
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			err := c.Stream(
				ctx,
				dests[i],
				svcName,
				svcMethod,
				vArgsChannels.Index(i).Interface(),
				vRepliesChannels.Index(i).Interface(),
			)
			errs[i] = err
			if err != nil {
				teeCancels[i]() // cancel context for this so that we close the send channel
			}
		}(i)
	}

	// Second, "tee" anything received from the argsChan into the channels
	// we created.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Close all sending channels when done.
		defer func() {
			for i := 0; i < n; i++ {
				vArgsChannels.Index(i).Close()
			}
		}()

		// Make a select with N cases (one per channel).
		cases := make([]reflect.SelectCase, n)
		for i := range cases {
			cases[i].Dir = reflect.SelectSend
		}

		// While there is something to receive from the argsChan.
		for {
			arg, ok := vArgsChan.Recv()
			if !ok {
				return
			}

			// Setup cases. If the context for a channel has not
			// been cancelled, prepare a send of the argument we
			// read from argsChan. Otherwise set Chan to nil
			// (effectively disables that case).
			validCases := 0
			for i := range cases {
				cases[i].Send = arg
				if teeCtxs[i].Err() == nil {
					cases[i].Chan = vArgsChannels.Index(i)
					validCases++
				} else {
					cases[i].Chan = reflect.ValueOf(nil)
				}
			}

			if validCases == 0 { // all our ops failed.
				go drainChannel(vArgsChan)
				return
			}

			// Make a select for all cases and call it
			// "validCases" times. This puts the arg value in all
			// still available channels, potentially blocking if
			// one of those channels has no buffer left.
			for i := 0; i < validCases; i++ {
				chosen, _, _ := reflect.Select(cases)
				cases[chosen].Chan = reflect.ValueOf(nil) // ignore this case
			}

			// We continue reading from the channel.
			// And repeat until no valid cases left.
			// otherwise we continue just draining the channel
		}
		// if we are here argsChan has been closed.
	}()

	// Third, "multiplex" anything received from the argsChan into the
	// channels we created.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Make a select with N cases (one per channel).
		cases := make([]reflect.SelectCase, n)
		for i := range cases {
			cases[i].Dir = reflect.SelectRecv
			cases[i].Chan = vRepliesChannels.Index(i)
		}

		validChannels := n
		for validChannels > 0 {
			chosen, v, ok := reflect.Select(cases)
			if !ok {
				cases[chosen].Chan = reflect.ValueOf(nil)
				validChannels--
				continue
			}
			vRepliesChan.Send(v)
		}

		// Close the response channels.
		vRepliesChan.Close()
	}()

	// Wait for everyone to finish.
	wg.Wait()
	return errs
}

func makeChanSliceOf(typ reflect.Type, cap int, buffer int) reflect.Value {
	chanSlice := reflect.MakeSlice(reflect.SliceOf(reflect.ChanOf(reflect.BothDir, typ.Elem())), 0, cap)
	for i := 0; i < cap; i++ {
		chanSlice = reflect.Append(
			chanSlice,
			reflect.MakeChan(reflect.ChanOf(reflect.BothDir, typ.Elem()), buffer),
		)
	}
	return chanSlice
}

// returns true if all arguments are the same number.
func checkMatchingLengths(l ...int) bool {
	if len(l) <= 1 {
		return true
	}

	for i := 1; i < len(l); i++ {
		if l[i-1] != l[i] {
			return false
		}
	}
	return true
}

// makeCall decides if a call can be performed. If it's a local
// call it will use the configured server if set.
func (c *Client) makeCall(call *Call) {
	logger.Debugf("makeCall: %s", call.SvcID)

	// Handle local RPC calls
	if call.Dest == "" || c.host == nil || call.Dest == c.host.ID() {
		logger.Debugf("local call: %s", call.SvcID)
		if c.server == nil {
			err := &clientError{"Cannot make local calls: server not set"}
			call.doneWithError(err)
			return
		}
		err := c.server.serverCall(call)
		call.doneWithError(err)
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

// send makes a REMOTE RPC call by initiating a libp2p stream to the
// destination and waiting for a response.
func (c *Client) send(call *Call) {
	logger.Debug("sending remote call")

	s, err := c.host.NewStream(call.ctx, call.Dest, c.protocol)
	if err != nil {
		call.doneWithError(newClientError(err))
		return
	}

	go call.watchContextWithStream(s)
	sWrap := wrapStream(s)

	logger.Debugf("sending RPC %s to %s", call.SvcID, call.Dest)
	if err := sWrap.enc.Encode(call.SvcID); err != nil {
		call.doneWithError(newClientError(err))
		s.Reset()
		return
	}

	// In this case, we have a single argument in the channel.
	if err := sWrap.enc.Encode(call.Args); err != nil {
		call.doneWithError(newClientError(err))
		s.Reset()
		return
	}

	if err := sWrap.w.Flush(); err != nil {
		call.doneWithError(newClientError(err))
		s.Reset()
		return
	}
	err = receiveResponse(sWrap, call)
	if err != nil {
		s.Reset()
		return
	}
	s.Close()
}

// receiveResponse reads a response to an RPC call
func receiveResponse(s *streamWrap, call *Call) error {
	logger.Debugf("waiting response for %s to %s", call.SvcID, call.Dest)
	var resp Response
	if err := s.dec.Decode(&resp); err != nil {
		call.doneWithError(newClientError(err))
		return err
	}

	if e := resp.Error; e != "" {
		err := responseError(resp.ErrType, e)
		call.setError(err)
		// we still try to read the body if possible
	}

	if err := s.dec.Decode(call.Reply); err != nil && err != io.EOF {
		call.doneWithError(newClientError(err))
		return err
	}
	call.done()
	return nil
}

// makeSteram performs a streaming call, either local or remote.
func (c *Client) makeStream(call *Call) {
	logger.Debugf("stream: %s", call.SvcID)

	// Handle local RPC calls
	if call.Dest == "" || c.host == nil || call.Dest == c.host.ID() {
		logger.Debugf("local call: %s", call.SvcID)
		if c.server == nil {
			err := &clientError{"Cannot make local calls: server not set"}
			call.doneWithError(err)
			return
		}
		err := c.server.serverStream(call)
		if err != nil {
			go drainChannel(call.StreamArgs)
		}
		call.doneWithError(err)
		return
	}

	// Handle remote RPC calls
	if c.host == nil {
		panic("no host set: cannot perform remote call")
	}
	if c.protocol == "" {
		panic("no protocol set: cannot perform remote call")
	}
	c.stream(call)
}

// stream makes a REMOTE RPC streaming call by initiating a libp2p stream to
// the destination, writing argument channel objects to it and reading the
// replies to the replies channel.
func (c *Client) stream(call *Call) {
	logger.Debug("streaming remote call")

	s, err := c.host.NewStream(call.ctx, call.Dest, c.protocol)
	if err != nil {
		call.doneWithError(newClientError(err))
		go drainChannel(call.StreamArgs)
		call.StreamReplies.Close()
		return
	}

	go call.watchContextWithStream(s)
	sWrap := wrapStream(s)

	// Send the service ID first. This may return an authorization error
	// for example.
	logger.Debugf("sending stream-RPC %s to %s", call.SvcID, call.Dest)
	if err := sWrap.enc.Encode(call.SvcID); err != nil {
		call.doneWithError(newClientError(err))
		s.Reset()
		go drainChannel(call.StreamArgs)
		call.StreamReplies.Close()
		return
	}

	// Flush that so that we become ready to read on the other side.
	if err := sWrap.w.Flush(); err != nil {
		call.doneWithError(newClientError(err))
		s.Reset()
		go drainChannel(call.StreamArgs)
		call.StreamReplies.Close()
		return
	}

	// Now we need to start writing arguments and reading.
	// Our context watcher will close the streams, we can therefore
	// not worry about contexts closures in our goroutines.
	var wg sync.WaitGroup
	wg.Add(2)

	// This goroutine sends things on the wire. It reads from the
	// arguments channel, encodes the object and flushes it.
	// Repeat until done or error.
	// Arguments channel is drained on error.
	go func() {
		// Close stream for writing when done
		defer wg.Done()
		defer s.CloseWrite()

		for {
			v, ok := call.StreamArgs.Recv()
			if !ok { // closed channel
				return
			}
			if err := sWrap.enc.Encode(v); err != nil {
				call.doneWithError(newClientError(err))
				// closing the args channel is responsibility
				// of the sender.
				s.Reset()
				go drainChannel(call.StreamArgs)
				return
			}

			// Flush it
			if err := sWrap.w.Flush(); err != nil {
				call.doneWithError(newClientError(err))
				s.Reset()
				go drainChannel(call.StreamArgs)
				return
			}
		}

	}()

	// This goroutine receives things from the wire.  First it reads a
	// Response (response can be considered reply "headers"). If it is an
	// error, then it aborts. Then it reads a reply object and sends it on
	// the reply channel.
	go func() {
		defer wg.Done()
		defer call.StreamReplies.Close()
		defer s.CloseRead()

		for {
			var resp Response

			err := sWrap.dec.Decode(&resp)
			if err == io.EOF {
				return
			}
			if err != nil {
				call.setError(newClientError(err))
				s.Reset()
				return
			}

			if resp.Error != "" {
				call.setError(responseError(resp.ErrType, resp.Error))
				s.Reset()
				return
			}

			// Now decode the data
			reply := reflect.New(call.StreamReplies.Type().Elem()).Elem().Interface()
			err = sWrap.dec.Decode(&reply)
			if err != nil {
				call.setError(newClientError(err))
				s.Reset()
				return
			}
			// Put element
			call.StreamReplies.Send(reflect.ValueOf(reply))
		}
	}()

	// Wait for send/receive routines to finish, cleanup and then signal
	// finalization of the Call.
	wg.Wait()
	s.Close()
	call.done()
}
