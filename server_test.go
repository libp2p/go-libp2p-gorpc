package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	logging "github.com/ipfs/go-log/v2"
)

func init() {
	logging.SetLogLevel("p2p-gorpc", "ERROR")
	//logging.SetDebugLogging()
}

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type ctxTracker struct {
	ctxMu sync.Mutex
	ctx   context.Context
}

func (ctxt *ctxTracker) setCtx(ctx context.Context) {
	if ctxt == nil {
		return
	}
	ctxt.ctxMu.Lock()
	defer ctxt.ctxMu.Unlock()
	ctxt.ctx = ctx
}

func (ctxt *ctxTracker) cancelled() bool {
	if ctxt == nil {
		return false
	}
	ctxt.ctxMu.Lock()
	defer ctxt.ctxMu.Unlock()
	return ctxt.ctx.Err() != nil
}

type Arith struct {
	ctxTracker *ctxTracker
}

func (t *Arith) Multiply(ctx context.Context, args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

// This uses non pointer args
func (t *Arith) Add(ctx context.Context, args Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

func (t *Arith) Divide(ctx context.Context, args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

func (t *Arith) GimmeError(ctx context.Context, args *Args, r *int) error {
	*r = 42
	return errors.New("an error")
}

func (t *Arith) Sleep(ctx context.Context, secs int, res *struct{}) error {
	t.ctxTracker.setCtx(ctx)
	tim := time.NewTimer(time.Duration(secs) * time.Second)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tim.C:
		return nil
	}
}

func (t *Arith) PrintHelloWorld(ctx context.Context, args struct{}, res *struct{}) error {
	t.ctxTracker.setCtx(ctx)
	fmt.Print("hello world!")
	return nil
}

func (t *Arith) DivideMyNumbers(ctx context.Context, args <-chan Args, quotients chan<- Quotient) error {
	t.ctxTracker.setCtx(ctx)
	for arg := range args {
		if arg.A == 1234 {
			time.Sleep(time.Second)
		}

		if arg.A == 666 {
			// it does not close(quotients) on purpose
			return errors.New("bad bad bad")
		}

		select {
		case <-ctx.Done():
			close(quotients)
			return ctx.Err()
		default:
		}

		if arg.B == 0 {
			close(quotients)
			return errors.New("divide by zero")
		}
		quo := Quotient{
			Quo: arg.A / arg.B,
			Rem: arg.A % arg.B,
		}
		quotients <- quo
	}
	close(quotients)
	return nil
}

func (t *Arith) DivideMyNumbersPointers(ctx context.Context, args <-chan *Args, quotients chan<- *Quotient) error {
	defer close(quotients)
	for arg := range args {
		if arg.B == 0 {
			return errors.New("divide by zero")
		}
		quo := Quotient{
			Quo: arg.A / arg.B,
			Rem: arg.A % arg.B,
		}
		quotients <- &quo
	}
	return nil
}

func makeRandomNodes() (h1, h2 host.Host) {
	h1, _ = libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/19998"),
	)
	h2, _ = libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/19999"),
	)

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.PermanentAddrTTL)
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.PermanentAddrTTL)

	return
}

func TestRegister(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()
	s := NewServer(h1, "rpc")
	var arith Arith

	err := s.Register(arith)
	if err == nil {
		t.Error("expected an error")
	}
	err = s.Register(&arith)
	if err != nil {
		t.Error(err)
	}
	// Re-register
	err = s.Register(&arith)
	if err == nil {
		t.Error("expected an error")
	}

}

func testCall(t *testing.T, servNode, clientNode host.Host, dest peer.ID) {
	s := NewServer(servNode, "rpc")
	c := NewClientWithServer(clientNode, "rpc", s)
	var arith Arith
	s.Register(&arith)

	var r int
	err := c.Call("", "Arith", "Multiply", &Args{2, 3}, &r)
	if err != nil {
		t.Fatal(err)
	}
	if r != 6 {
		t.Error("result is:", r)
	}

	var a int
	err = c.Call("", "Arith", "Add", Args{2, 3}, &a)
	if err != nil {
		t.Fatal(err)
	}
	if a != 5 {
		t.Error("result is:", a)
	}

	var q Quotient
	err = c.Call(dest, "Arith", "Divide", &Args{20, 6}, &q)
	if err != nil {
		t.Fatal(err)
	}
	if q.Quo != 3 || q.Rem != 2 {
		t.Error("bad division")
	}
}

func TestCall(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testCall(t, h1, h2, "")
	})
	t.Run("remote", func(t *testing.T) {
		testCall(t, h1, h2, h1.ID())
	})
}

func TestErrorResponse(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	var arith Arith
	s.Register(&arith)

	t.Run("remote", func(t *testing.T) {
		var r int
		c := NewClientWithServer(h2, "rpc", s)
		err := c.Call(h1.ID(), "Arith", "GimmeError", &Args{1, 2}, &r)
		if err == nil || err.Error() != "an error" {
			t.Error("expected different error")
		}
		if r != 42 {
			t.Error("response should be set even on error")
		}
	})

	t.Run("local", func(t *testing.T) {
		var r int
		c := NewClientWithServer(h1, "rpc", s)
		err := c.Call(h1.ID(), "Arith", "GimmeError", &Args{1, 2}, &r)
		if err == nil || err.Error() != "an error" {
			t.Error("expected different error")
		}
		if r != 42 {
			t.Error("response should be set even on error")
		}
	})
}

func TestNonRPCError(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	var arith Arith
	s.Register(&arith)

	t.Run("local non rpc error", func(t *testing.T) {
		var r int
		c := NewClientWithServer(h1, "rpc", s)
		err := c.Call(h1.ID(), "Arith", "GimmeError", &Args{1, 2}, &r)
		if err != nil {
			if IsRPCError(err) {
				t.Log(err)
				t.Error("expected non rpc error")
			}
		}
	})

	t.Run("local rpc error", func(t *testing.T) {
		var r int
		c := NewClientWithServer(h1, "rpc", s)
		err := c.Call(h1.ID(), "Arith", "ThisIsNotAMethod", &Args{1, 2}, &r)
		if err != nil {
			if !IsRPCError(err) {
				t.Log(err)
				t.Error("expected rpc error")
			}
		}
	})

	t.Run("remote non rpc error", func(t *testing.T) {
		var r int
		c := NewClientWithServer(h2, "rpc", s)
		err := c.Call(h1.ID(), "Arith", "GimmeError", &Args{1, 2}, &r)
		if err != nil {
			if IsRPCError(err) {
				t.Log(err)
				t.Error("expected non rpc error")
			}
		}
	})

	t.Run("remote rpc error", func(t *testing.T) {
		var r int
		c := NewClientWithServer(h2, "rpc", s)
		err := c.Call(h1.ID(), "Arith", "ThisIsNotAMethod", &Args{1, 2}, &r)
		if err != nil {
			if !IsRPCError(err) {
				t.Log(err)
				t.Error("expected rpc error")
			}
		}
	})
}

func testCallContext(t *testing.T, servHost, clientHost host.Host, dest peer.ID) {
	s := NewServer(servHost, "rpc")
	c := NewClientWithServer(clientHost, "rpc", s)

	var arith Arith
	arith.ctxTracker = &ctxTracker{}
	s.Register(&arith)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second/2)
	defer cancel()
	err := c.CallContext(ctx, dest, "Arith", "Sleep", 5, &struct{}{})
	if err == nil {
		t.Fatal("expected an error")
	}

	if !strings.Contains(err.Error(), "context") {
		t.Error("expected a context error:", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !arith.ctxTracker.cancelled() {
		t.Error("expected ctx cancellation in the function")
	}
}

func TestCallContext(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testCallContext(t, h1, h2, h2.ID())
	})

	t.Run("remote", func(t *testing.T) {
		testCallContext(t, h1, h2, h1.ID())
	})

	t.Run("async", func(t *testing.T) {
		s := NewServer(h1, "rpc")
		c := NewClientWithServer(h2, "rpc", s)

		var arith Arith
		arith.ctxTracker = &ctxTracker{}
		s.Register(&arith)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second/2)
		defer cancel()

		done := make(chan *Call, 1)
		err := c.GoContext(ctx, h1.ID(), "Arith", "Sleep", 5, &struct{}{}, done)
		if err != nil {
			t.Fatal(err)
		}

		call := <-done
		if call.Error == nil || !strings.Contains(call.Error.Error(), "context") {
			t.Error("expected a context error:", err)
		}
	})
}

func TestMultiCall(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	c := NewClientWithServer(h2, "rpc", s)
	var arith Arith
	s.Register(&arith)

	replies := make([]int, 2)
	ctxs := make([]context.Context, 2)
	repliesInt := make([]interface{}, 2)
	for i := range repliesInt {
		repliesInt[i] = &replies[i]
		ctxs[i] = context.Background()
	}

	errs := c.MultiCall(
		ctxs,
		[]peer.ID{h1.ID(), h2.ID()},
		"Arith",
		"Multiply",
		&Args{2, 3},
		repliesInt,
	)

	if len(errs) != 2 {
		t.Fatal("expected two errs")
	}

	for _, err := range errs {
		if err != nil {
			t.Error(err)
		}
	}

	for _, reply := range replies {
		if reply != 6 {
			t.Error("expected 2*3=6")
		}
	}
}

func TestMultiGo(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	c := NewClientWithServer(h2, "rpc", s)
	var arith Arith
	s.Register(&arith)

	replies := make([]int, 2)
	ctxs := make([]context.Context, 2)
	repliesInt := make([]interface{}, 2)
	dones := make([]chan *Call, 2)
	for i := range repliesInt {
		repliesInt[i] = &replies[i]
		ctxs[i] = context.Background()
		dones[i] = make(chan *Call, 1)
	}

	err := c.MultiGo(
		ctxs,
		[]peer.ID{h1.ID(), h2.ID()},
		"Arith",
		"Multiply",
		&Args{2, 3},
		repliesInt,
		dones,
	)

	if err != nil {
		t.Error(err)
	}

	<-dones[0]
	<-dones[1]

	for _, reply := range replies {
		if reply != 6 {
			t.Error("expected 2*3=6")
		}
	}
}

func testDecodeContext(t *testing.T, servHost, clientHost host.Host, dest peer.ID) {
	s := NewServer(servHost, "rpc")
	c := NewClientWithServer(clientHost, "rpc", s)

	var arith Arith
	arith.ctxTracker = &ctxTracker{}
	s.Register(&arith)

	ctx := context.Background()
	var res int
	err := c.CallContext(ctx, dest, "Arith", "Add", Args{1, 1}, &res)
	if err != nil {
		t.Fatal(err)
	}

}

func TestDecodeContext(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testDecodeContext(t, h1, h2, h2.ID())
	})

	t.Run("remote", func(t *testing.T) {
		testDecodeContext(t, h1, h2, h1.ID())
	})
}

func TestAuthorization(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	h3, _ := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/19997"),
	)
	h3.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.PermanentAddrTTL)

	authorizationFunc := AuthorizeWithMap(
		map[peer.ID]map[string]bool{
			h2.ID(): {
				"Arith.Multiply": true,
			},
		},
	)

	s := NewServer(h1, "rpc", WithAuthorizeFunc(authorizationFunc))
	c := NewClientWithServer(h2, "rpc", s)
	var arith Arith
	s.Register(&arith)

	dest := h1.ID()

	var r int
	err := c.Call(dest, "Arith", "Multiply", &Args{2, 3}, &r)
	if err != nil {
		t.Fatal(err)
	}
	if r != 6 {
		t.Error("result is:", r)
	}

	var q Quotient
	err = c.Call(dest, "Arith", "Divide", &Args{20, 6}, &q)
	if err == nil {
		t.Fatal("expected error instead")
	}
	if !IsAuthorizationError(err) {
		t.Error("expected authorization error, but found", responseErrorType(err))
	}

	c1 := NewClientWithServer(h3, "rpc", s)
	err = c1.Call(dest, "Arith", "Multiply", &Args{2, 3}, &r)
	if err == nil {
		t.Fatal("expected error instead")
	}
	if !IsAuthorizationError(err) {
		t.Error("expected authorization error, but found", responseErrorType(err))
	}

	// Authorization should not impact while accessing methods locally.
	// All methods should be allowed locally.
	t.Run("local", func(t *testing.T) {
		testCall(t, h1, h2, "")
	})

}

func testRequestSenderPeerIDContext(t *testing.T, servHost, clientHost host.Host, dest peer.ID) {
	s := NewServer(servHost, "rpc")
	c := NewClientWithServer(clientHost, "rpc", s)

	var arith Arith
	arith.ctxTracker = &ctxTracker{}
	err := s.Register(&arith)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Call(dest, "Arith", "PrintHelloWorld", struct{}{}, &struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	p, err := GetRequestSender(arith.ctxTracker.ctx)
	if err != nil {
		t.Fatal(err)
	}

	if dest == "" || dest == clientHost.ID() {
		if p != servHost.ID() {
			t.Errorf("invalid peer id of request sender on local call: have: %s, want: %s", p, servHost.ID())
		}
	} else {
		if p != clientHost.ID() {
			t.Errorf("invalid peer id of request sender on remote call: have: %s, want: %s", p, clientHost.ID())
		}
	}
}

func TestRequestSenderPeerIDContext(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testRequestSenderPeerIDContext(t, h1, h2, h2.ID())
	})

	t.Run("remote", func(t *testing.T) {
		testRequestSenderPeerIDContext(t, h1, h2, h1.ID())
	})
}

func testStream(t *testing.T, servHost, clientHost host.Host, dest peer.ID) {
	s := NewServer(servHost, "rpc")
	c := NewClientWithServer(clientHost, "rpc", s)
	var arith Arith
	s.Register(&arith)

	numbers := make(chan Args, 10)
	quotients := make(chan Quotient, 10)

	numbers <- Args{2, 3}
	numbers <- Args{6, 2}
	numbers <- Args{9, 5}
	close(numbers)

	err := c.Stream(context.Background(), dest, "Arith", "DivideMyNumbers", numbers, quotients)
	if err != nil {
		t.Fatal(err)
	}

	if len(quotients) != 3 {
		t.Fatal("expected 3 quotients waiting in channel")
	}

	q := <-quotients
	if q.Quo != 0 || q.Rem != 2 {
		t.Error("wrong result")
	}

	q = <-quotients
	if q.Quo != 3 || q.Rem != 0 {
		t.Error("wrong result")
	}

	q = <-quotients
	if q.Quo != 1 || q.Rem != 4 {
		t.Error("wrong result")
	}

	q, ok := <-quotients
	if ok {
		t.Error("channel should have been closed")
	}

	// Now test with pointer arguments
	numbersP := make(chan *Args, 10)
	quotientsP := make(chan *Quotient, 10)

	numbersP <- &Args{2, 3}
	close(numbersP)

	err = c.Stream(context.Background(), dest, "Arith", "DivideMyNumbersPointers", numbersP, quotientsP)
	if err != nil {
		t.Fatal(err)
	}
	qP := <-quotientsP
	if qP.Quo != 0 || qP.Rem != 2 {
		t.Error("wrong result")
	}
	_, ok = <-quotientsP
	if ok {
		t.Error("channel should be closed")
	}
}

func TestStream(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testStream(t, h1, h2, h2.ID())
	})

	t.Run("remote", func(t *testing.T) {
		testStream(t, h1, h2, h1.ID())
	})
}

func testStreamError(t *testing.T, servHost, clientHost host.Host, dest peer.ID) {
	s := NewServer(servHost, "rpc")
	c := NewClientWithServer(clientHost, "rpc", s)
	var arith Arith
	s.Register(&arith)

	numbers := make(chan Args, 10)
	quotients := make(chan Quotient, 10)

	numbers <- Args{2, 3}
	numbers <- Args{6, 0}
	close(numbers)

	err := c.Stream(context.Background(), dest, "Arith", "DivideMyNumbers", numbers, quotients)
	if err == nil {
		t.Error("expected an error")
	}

	if err.Error() != "divide by zero" {
		t.Error("wrong error message")
	}

	// sometimes the error comes in before the first response is posted on
	// the channel, sometimes it doesn't. In any case, channel should be
	// closed soon.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wait := make(chan struct{})
	go func() {
		defer func() { wait <- struct{}{} }()
		for {
			select {
			case <-ctx.Done():
				t.Error("should have drained the channel")
				return
			case _, ok := <-quotients:
				if !ok {
					return
				}
			}
		}

	}()
	<-wait
}

func TestStreamError(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testStreamError(t, h1, h2, h2.ID())
	})

	t.Run("remote", func(t *testing.T) {
		testStreamError(t, h1, h2, h1.ID())
	})
}

func testStreamCancel(t *testing.T, servHost, clientHost host.Host, dest peer.ID) {
	s := NewServer(servHost, "rpc")
	c := NewClientWithServer(clientHost, "rpc", s)
	var arith Arith
	s.Register(&arith)
	numbers := make(chan Args, 10)
	quotients := make(chan Quotient, 10)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		numbers <- Args{2, 3}
		numbers <- Args{1234, 5}
		close(numbers)

		err := c.Stream(ctx, dest, "Arith", "DivideMyNumbers", numbers, quotients)
		if err == nil {
			t.Error("expected an error")
		}

		if err.Error() != ctx.Err().Error() {
			t.Error("wrong error message")
		}
	}()

	// channel should be closed.
	ctx2, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx2.Done():
				t.Error("should have drained the channel")
				return
			case _, ok := <-quotients:
				if !ok {
					return
				}
			}
		}

	}()
	wg.Wait()
}

func TestStreamCancel(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testStreamCancel(t, h1, h2, h2.ID())
	})

	t.Run("remote", func(t *testing.T) {
		testStreamCancel(t, h1, h2, h1.ID())
	})
}

func TestMultiStream(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	s2 := NewServer(h2, "rpc")
	c := NewClientWithServer(h1, "rpc", s)
	var arith Arith
	s.Register(&arith)
	s2.Register(&arith)

	ctx := context.Background()
	dests := make([]peer.ID, 2)
	dests[0] = h1.ID()
	dests[1] = h2.ID()

	numbers := make(chan Args, 10)
	quotients := make(chan Quotient, 10)

	numbers <- Args{10, 3}
	numbers <- Args{10, 3}
	close(numbers)

	errs := c.MultiStream(ctx, dests, "Arith", "DivideMyNumbers", numbers, quotients)
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if len(quotients) != 4 {
		t.Error("not all responses arrived")
	}

	for r := range quotients {
		if r.Quo != 3 || r.Rem != 1 {
			t.Error("wrong result")
		}
	}
}

func TestMultiStreamErrors(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	s2 := NewServer(h2, "rpc")
	c := NewClientWithServer(h1, "rpc", s)
	var arith Arith
	s.Register(&arith)
	s2.Register(&arith)

	ctx := context.Background()
	dests := make([]peer.ID, 2)
	dests[0] = h1.ID()
	dests[1] = h2.ID()

	numbers := make(chan Args, 10)
	quotients := make(chan Quotient, 10)

	numbers <- Args{10, 0}
	numbers <- Args{10, 0}
	close(numbers)

	errs := c.MultiStream(ctx, dests, "Arith", "DivideMyNumbers", numbers, quotients)
	for _, err := range errs {
		if err == nil {
			t.Fatal("expected errors")
		}
	}
}

func TestMultiStreamCancel(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	s2 := NewServer(h2, "rpc")
	c := NewClientWithServer(h1, "rpc", s)
	var arith Arith
	s.Register(&arith)
	s2.Register(&arith)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	dests := make([]peer.ID, 2)
	dests[0] = h1.ID()
	dests[1] = h2.ID()
	numbers := make(chan Args, 10)
	quotients := make(chan Quotient, 10)

	numbers <- Args{10, 2}
	numbers <- Args{1234, 3}
	close(numbers)

	errs := c.MultiStream(ctx, dests, "Arith", "DivideMyNumbers", numbers, quotients)
	for _, err := range errs {
		if err == nil {
			t.Fatal("expected errors")
			continue
		}
		if err.Error() != ctx.Err().Error() {
			t.Error("expected context error")
		}
	}
}

// the client cancels the request but does not close the sending channel. Things
// should return.
func testStreamClientMisbehave(t *testing.T, servHost, clientHost host.Host, dest peer.ID) {
	s := NewServer(servHost, "rpc")
	c := NewClientWithServer(clientHost, "rpc", s)
	var arith Arith
	arith.ctxTracker = &ctxTracker{}
	s.Register(&arith)
	numbers := make(chan Args)
	quotients := make(chan Quotient, 10)

	go func() {
		for {
			numbers <- Args{1234, 5} // slow operation
			time.Sleep(100 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := c.Stream(ctx, dest, "Arith", "DivideMyNumbers", numbers, quotients)
	if err == nil {
		t.Error("expected an error")
	}

	if err.Error() != ctx.Err().Error() {
		t.Error("wrong error message")
	}

	time.Sleep(1000 * time.Millisecond)
	if !arith.ctxTracker.cancelled() {
		t.Error("expected ctx cancellation in the function")
	}
}

func TestStreamClientMisbehave(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testStreamClientMisbehave(t, h1, h2, h2.ID())
	})

	t.Run("remote", func(t *testing.T) {
		testStreamClientMisbehave(t, h1, h2, h1.ID())
	})
}

// the server errors but does not cancel the reply channel.
func testStreamServerMisbehave(t *testing.T, servHost, clientHost host.Host, dest peer.ID) {
	s := NewServer(servHost, "rpc")
	c := NewClientWithServer(clientHost, "rpc", s)
	var arith Arith
	s.Register(&arith)
	numbers := make(chan Args, 2)
	quotients := make(chan Quotient, 10)

	go func() {
		numbers <- Args{2, 3}
		numbers <- Args{666, 1} // causes error without closing channel
		for {
			numbers <- Args{1234, 5} // slow operation
			time.Sleep(100 * time.Millisecond)
		}
	}()

	err := c.Stream(context.Background(), dest, "Arith", "DivideMyNumbers", numbers, quotients)
	if err == nil {
		t.Error("expected an error")
	}

	if err.Error() != "bad bad bad" {
		t.Error("wrong error message")
	}

}

func TestStreamServerMisbehave(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	t.Run("local", func(t *testing.T) {
		testStreamServerMisbehave(t, h1, h2, h2.ID())
	})

	t.Run("remote", func(t *testing.T) {
		testStreamServerMisbehave(t, h1, h2, h1.ID())
	})
}
