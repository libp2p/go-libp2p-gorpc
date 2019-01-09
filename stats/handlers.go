package stats

import "context"

// RPCTagInfo defines the relevant information needed by RPC context tagger.
type RPCTagInfo struct {
	// FullMethodName is the RPC method in the format of /package.service/method.
	FullMethodName string
}

// Handler defines the interface for the related stats handling (e.g., RPCs, connections).
type Handler interface {
	// TagRPC can attach some information to the given context.
	// The context used for the rest lifetime of the RPC will be derived from
	// the returned context.
	TagRPC(context.Context, *RPCTagInfo) context.Context
	// HandleRPC processes the RPC stats.
	HandleRPC(context.Context, RPCStats)
}
