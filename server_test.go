package rpc

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	logging "github.com/ipfs/go-log"
	crypto "github.com/libp2p/go-libp2p-crypto"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	swarm "github.com/libp2p/go-libp2p-swarm"
	basic "github.com/libp2p/go-libp2p/p2p/host/basic"
	multiaddr "github.com/multiformats/go-multiaddr"
)

func init() {
	logging.SetLogLevel("p2p-gorpc", "DEBUG")
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
	ctxt.ctxMu.Lock()
	defer ctxt.ctxMu.Unlock()
	ctxt.ctx = ctx
}

func (ctxt *ctxTracker) cancelled() bool {
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

func makeRandomNodes() (h1, h2 host.Host) {
	priv1, pub1, _ := crypto.GenerateKeyPair(crypto.RSA, 2048)
	pid1, _ := peer.IDFromPublicKey(pub1)
	maddr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/19998")

	priv2, pub2, _ := crypto.GenerateKeyPair(crypto.RSA, 2048)
	pid2, _ := peer.IDFromPublicKey(pub2)
	maddr2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/19999")

	ps1 := peerstore.NewPeerstore()
	ps2 := peerstore.NewPeerstore()
	ps1.AddPubKey(pid1, pub1)
	ps1.AddPrivKey(pid1, priv1)
	ps1.AddPubKey(pid2, pub2)
	ps1.AddPrivKey(pid2, priv2)
	ps1.AddAddrs(pid2, []multiaddr.Multiaddr{maddr2}, peerstore.PermanentAddrTTL)

	ps2.AddPubKey(pid1, pub1)
	ps2.AddPrivKey(pid1, priv1)
	ps2.AddPubKey(pid2, pub2)
	ps2.AddPrivKey(pid2, priv2)
	ps2.AddAddrs(pid1, []multiaddr.Multiaddr{maddr1}, peerstore.PermanentAddrTTL)

	ctx := context.Background()
	n1, _ := swarm.NewNetwork(
		ctx,
		[]multiaddr.Multiaddr{maddr1},
		pid1,
		ps1,
		nil)
	n2, _ := swarm.NewNetwork(
		ctx,
		[]multiaddr.Multiaddr{maddr2},
		pid2,
		ps2,
		nil)

	h1 = basic.New(n1)
	h2 = basic.New(n2)
	time.Sleep(time.Second)
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

func TestRemote(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()
	s := NewServer(h1, "rpc")
	c := NewClientWithServer(h2, "rpc", s)
	var arith Arith
	s.Register(&arith)

	var r int
	err := c.Call(h1.ID(), "Arith", "Multiply", &Args{2, 3}, &r)
	if err != nil {
		t.Fatal(err)
	}
	if r != 6 {
		t.Error("result is:", r)
	}

	var a int
	err = c.Call(h1.ID(), "Arith", "Add", Args{2, 3}, &a)
	if err != nil {
		t.Fatal(err)
	}
	if a != 5 {
		t.Error("result is:", a)
	}

	var q Quotient
	err = c.Call(h1.ID(), "Arith", "Divide", &Args{20, 6}, &q)
	if err != nil {
		t.Fatal(err)
	}
	if q.Quo != 3 || q.Rem != 2 {
		t.Error("bad division")
	}
}

func TestLocal(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	c := NewClientWithServer(h1, "rpc", s)
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
	err = c.Call(h1.ID(), "Arith", "Divide", &Args{20, 6}, &q)
	if err != nil {
		t.Fatal(err)
	}
	if q.Quo != 3 || q.Rem != 2 {
		t.Error("bad division")
	}
}

func TestErrorResponse(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()

	s := NewServer(h1, "rpc")
	var arith Arith
	s.Register(&arith)

	var r int
	// test remote
	c := NewClientWithServer(h2, "rpc", s)
	err := c.Call(h1.ID(), "Arith", "GimmeError", &Args{1, 2}, &r)
	if err == nil || err.Error() != "an error" {
		t.Error("expected different error")
	}
	if r != 42 {
		t.Error("response should be set even on error")
	}

	// test local
	c = NewClientWithServer(h1, "rpc", s)
	err = c.Call(h1.ID(), "Arith", "GimmeError", &Args{1, 2}, &r)
	if err == nil || err.Error() != "an error" {
		t.Error("expected different error")
	}
	if r != 42 {
		t.Error("response should be set even on error")
	}
}

func TestCallContextLocal(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()
	s := NewServer(h1, "rpc")
	c := NewClientWithServer(h2, "rpc", s)
	var arith Arith
	arith.ctxTracker = &ctxTracker{}
	s.Register(&arith)

	// Local
	ctx, cancel := context.WithTimeout(context.Background(), time.Second/2)
	defer cancel()
	err := c.CallContext(ctx, h2.ID(), "Arith", "Sleep", 5, &struct{}{})
	if err == nil {
		t.Fatal("expected an error")
	}

	if !strings.Contains(err.Error(), "context") {
		t.Error("expected a context error:", err)
	}

	if !arith.ctxTracker.cancelled() {
		t.Error("expected ctx cancellation in the function")
	}
}

func TestCallContextRemote(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()
	s := NewServer(h1, "rpc")
	c := NewClient(h2, "rpc")
	var arith Arith
	arith.ctxTracker = &ctxTracker{}
	s.Register(&arith)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := c.CallContext(ctx, h1.ID(), "Arith", "Sleep", 5, &struct{}{})
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

func TestGoContext(t *testing.T) {
	h1, h2 := makeRandomNodes()
	defer h1.Close()
	defer h2.Close()
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
}
