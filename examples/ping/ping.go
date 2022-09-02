package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"time"

	"log"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	gorpc "github.com/libp2p/go-libp2p-gorpc"

	multiaddr "github.com/multiformats/go-multiaddr"
)

type PingArgs struct {
	Data []byte
}
type PingReply struct {
	Data []byte
}
type PingService struct{}

func (t *PingService) Ping(ctx context.Context, argType PingArgs, replyType *PingReply) error {
	log.Println("Received a Ping call")
	replyType.Data = argType.Data
	return nil
}

func createPeer(listenAddr string) host.Host {
	// Create a new libp2p host
	h, err := libp2p.New(libp2p.ListenAddrStrings(listenAddr))
	if err != nil {
		panic(err)
	}
	return h
}

var protocolID = protocol.ID("/p2p/rpc/ping")

func startServer() {
	log.Println("Launching host")
	host := createPeer("/ip4/0.0.0.0/tcp/9000")

	log.Printf("Hello World, my hosts ID is %s\n", host.ID().Pretty())
	for _, addr := range host.Addrs() {
		ipfsAddr, err := multiaddr.NewMultiaddr("/ipfs/" + host.ID().Pretty())
		if err != nil {
			panic(err)
		}
		peerAddr := addr.Encapsulate(ipfsAddr)
		log.Printf("I'm listening on %s\n", peerAddr)
	}

	rpcHost := gorpc.NewServer(host, protocolID)

	svc := PingService{}
	err := rpcHost.Register(&svc)
	if err != nil {
		panic(err)
	}

	fmt.Println("Done")

	for {
		time.Sleep(time.Second * 1)
	}
}

func startClient(host string, pingCount, randomDataSize int) {
	fmt.Println("Launching client")
	client := createPeer("/ip4/0.0.0.0/tcp/9001")
	fmt.Printf("Hello World, my hosts ID is %s\n", client.ID().Pretty())
	ma, err := multiaddr.NewMultiaddr(host)
	if err != nil {
		panic(err)
	}
	peerInfo, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	err = client.Connect(ctx, *peerInfo)
	if err != nil {
		panic(err)
	}
	rpcClient := gorpc.NewClient(client, protocolID)
	numCalls := 0
	durations := []time.Duration{}
	betweenPingsSleep := time.Second * 1

	for numCalls < pingCount {
		var reply PingReply
		var args PingArgs

		c := randomDataSize
		b := make([]byte, c)
		_, err := rand.Read(b)
		if err != nil {
			panic(err)
		}

		args.Data = b

		time.Sleep(betweenPingsSleep)
		startTime := time.Now()
		err = rpcClient.Call(peerInfo.ID, "PingService", "Ping", args, &reply)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(reply.Data, b) {
			panic("Received wrong amount of bytes back!")
		}
		endTime := time.Now()
		diff := endTime.Sub(startTime)
		fmt.Printf("%d bytes from %s (%s): seq=%d time=%s\n", c, peerInfo.ID.String(), peerInfo.Addrs[0].String(), numCalls+1, diff)
		numCalls += 1
		durations = append(durations, diff)
	}

	totalDuration := int64(0)
	for _, dur := range durations {
		totalDuration = totalDuration + dur.Nanoseconds()
	}
	averageDuration := totalDuration / int64(len(durations))
	fmt.Printf("Average duration for ping reply: %s\n", time.Duration(averageDuration))

}

func main() {

	var mode string
	var host string
	var count int
	var size int
	flag.StringVar(&mode, "mode", "", "host or client mode")
	flag.StringVar(&host, "host", "", "address of host to connect to")
	flag.IntVar(&count, "count", 10, "number of pings to make")
	flag.IntVar(&size, "size", 64, "size of random data in ping message")
	flag.Parse()

	if mode == "" {
		log.Fatal("You need to specify '-mode' to be either 'host' or 'client'")
	}

	if mode == "host" {
		startServer()
		return
	}
	if mode == "client" {
		if host == "" {
			log.Fatal("You need to specify '-host' when running as a client")
		}
		startClient(host, count, size)
		return
	}
	log.Fatal("Mode '" + mode + "' not recognized. It has to be either 'host' or 'client'")

}
