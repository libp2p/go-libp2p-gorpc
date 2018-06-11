# go-libp2p-gorpc ping example

Quick example how to build a ping service with go-libp2p-gorpc

This example has two parts, the `host` and the `client`. You can switch between
them with the `-mode` flag that accepts either `host` or `client` as value.

## Usage

Have two terminal windows open in the `examples/ping` directory. In the first
one, run:

```
$ go run ping.go -mode host
```

And then copy one of the "I'm listening on" addresses. In this example, we use
the `127.0.0.1` one which ends up being:

```
/ip4/127.0.0.1/tcp/9000/ipfs/QmTwhWUFdY8NvhmLxE9CzPm29zC9bzfoMGAz2SFV5cb26d
```

Now in the second terminal window, run:

```
$ go run ping.go -mode client -host /ip4/127.0.0.1/tcp/9000/ipfs/QmTwhWUFdY8NvhmLxE9CzPm29zC9bzfoMGAz2SFV5cb26d
```

And you should start seeing log messages showing the duration of each ping, and
finally a average of 10 pings.

```
2018/06/10 12:52:44 Launching client
2018/06/10 12:52:44 Hello World, my hosts ID is Qmapkii8GMB2fMUT66yds9surJUdsZHMtygFSFhPnHa14K
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=1 time=1.404259ms
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=2 time=1.338412ms
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=3 time=892.567µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=4 time=505.573µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=5 time=565.036µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=6 time=765.652µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=7 time=1.296701ms
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=8 time=804.552µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=9 time=733.054µs
64 bytes from <peer.ID UGZS55> (/ip4/127.0.0.1/tcp/9000): seq=10 time=688.807µs
Average duration for ping reply: 899.461µs
```

## Explanation

Here is some of the important code snippets from this example. Keep in mind
that some information here is hard-coded and error-handling is omitted for brevity
and is not a example of production-ready code. To see a more real version of
this code, please check the `ping.go` file inside this directory.

### Host

First we create our libp2p host:

```golang
host, _ := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/9000"))
```

After that, we create our gorpc host that will received calls

```golang
rpcHost := gorpc.NewServer(host, protocol.ID("/p2p/rpc/ping"))
```

Now, we need to have three structs and one method to be able to respond to the
RPC calls from the client. The arguments and the reply only has one argument,
`Data` which is being sent from the client, and replied back in the response.

```golang
type PingArgs struct {
	Data []byte
}
type PingReply struct {
	Data []byte
}
type PingService struct{}

func (t *PingService) Ping(argType PingArgs, replyType *PingReply) error {
	replyType.Data = argType.Data
	return nil
}
```

Once we have those defined, we can register our PingService with the RPCHost

```golang
svc := PingService{}
rpcHost.Register(&svc)
```

Now our host is ready to reply to pings from the client.

### Client

Again, let's first create our libp2p peer

```golang
client, _ := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/9001"))
```

Now we need to first connect to the host that we created before.

```golang
host := "/ip4/127.0.0.1/tcp/9000/ipfs/QmUGZS556mhYSSrFGJpxtt33QQuRDjhsYvFNCW1V3A4wjL"
ma, _ := multiaddr.NewMultiaddr(host)
peerInfo, _ := peerstore.InfoFromP2pAddr(ma)
ctx := context.Background()
client.Connect(ctx, *peerInfo)
```

And now we can create our gorpc client with the newly created libp2p client

```golang
rpcClient := gorpc.NewClient(client, protocol.ID("/p2p/rpc/ping"))
```

Then we can start making our rpc call. We start by defining our reply and arguments

```golang
var reply PingReply
var args PingArgs
```

To make sure that we actually make the call correct, we add some random data
in the arguments so we can check it when we get the reply.

```golang
c := 64
b := make([]byte, c)
rand.Read(b)
args.Data = b
```

Now we can finally make the call itself! Keep in mind this is a blocking call,
and it'll fill out `reply` for you.

```golang
rpcClient.Call(peerInfo.ID, "PingService", "Ping", args, &reply)
```

Once the call above has finished, `reply.Data` should now have the same data
as we had before

```golang
if bytes.Equal(reply.Data, b) {
  fmt.Println("Got the same bytes back as we sent!")
}
```
