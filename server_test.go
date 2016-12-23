package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	host "gx/ipfs/QmPTGbC34bPKaUm9wTxBo7zSCac7pDuG42ZmnXC718CKZZ/go-libp2p-host"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	multiaddr "gx/ipfs/QmUAQaWbKxGCUTuoQVvvicbQNZ9APF5pDGWyAZSe93AtKH/go-multiaddr"
	swarm "gx/ipfs/QmWfxnAiQ5TnnCgiX9ikVUKFNHRgGhbgKdx5DoKPELD7P4/go-libp2p-swarm"
	basic "gx/ipfs/QmbzCT1CwxVZ2ednptC9RavuJe7Bv8DDi2Ne89qUrA37XM/go-libp2p/p2p/host/basic"
	peerstore "gx/ipfs/QmeXj9VAjmYQZxpmVz7VzccbJrpmr8qkCDSjfVNsPTWTYU/go-libp2p-peerstore"
	peer "gx/ipfs/QmfMmLGoKzCHDN7cGgk64PJr4iipzidDRME8HABSJqvmhC/go-libp2p-peer"
	crypto "gx/ipfs/QmfWDLQjGjVe4fr5CoztYW2DYYjRysMJrFe1RCsXLPTf46/go-libp2p-crypto"
)

func init() {
	logging.SetLogLevel("rpc", "DEBUG")
}

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
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
	err := c.Call(h1.ID(), "Arith", "Multiply", &Args{2, 3}, &r)
	if err != nil {
		t.Fatal(err)
	}
	if r != 6 {
		t.Error("result is:", r)
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
