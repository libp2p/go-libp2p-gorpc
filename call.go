package rpc

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

// Call represents an active RPC. Calls are used to indicate completion
// of RPC requests and are returned within the provided channel in
// the Go() functions.
type Call struct {
	ctx    context.Context
	cancel func()

	finishedMu sync.RWMutex
	finished   bool

	Dest  peer.ID
	SvcID ServiceID   // The name of the service and method to call.
	Args  interface{} // The argument to the function (*struct).
	Reply interface{} // The reply from the function (*struct).
	Done  chan *Call  // Strobes when call is complete.

	errorMu sync.Mutex
	Error   error // After completion, the error status.
}

func newCall(ctx context.Context, dest peer.ID, svcName, svcMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	ctx2, cancel := context.WithCancel(ctx)
	return &Call{
		ctx:    ctx2,
		cancel: cancel,
		Dest:   dest,
		SvcID:  ServiceID{svcName, svcMethod},
		Args:   args,
		Reply:  reply,
		Error:  nil,
		Done:   done,
	}
}

// done places the completed call in the done channel.
func (call *Call) done() {
	call.finishedMu.Lock()
	call.finished = true
	call.finishedMu.Unlock()

	select {
	case call.Done <- call:
		// ok
	default:
		logger.Debugf("discarding %s.%s call reply",
			call.SvcID.Name, call.SvcID.Method)
	}
	call.cancel()
}

func (call *Call) doneWithError(err error) {
	if err != nil {
		logger.Error(err)
		call.setError(err)
	}
	call.done()
}

func (call *Call) isFinished() bool {
	call.finishedMu.RLock()
	defer call.finishedMu.RUnlock()
	return call.finished
}

// watch context will wait for a context cancellation
// and close the stream.
func (call *Call) watchContextWithStream(s network.Stream) {
	select {
	case <-call.ctx.Done():
		if !call.isFinished() { // context was cancelled not by us
			logger.Debug("call context is done before finishing")
			// FullClose() instead of Reset(). This lets the other
			// write to the stream without printing errors to
			// the console (graceful fail) and eventually will
			// reset.
			go helpers.FullClose(s)
			call.doneWithError(call.ctx.Err())
		}
	}
}

func (call *Call) setError(err error) {
	call.errorMu.Lock()
	defer call.errorMu.Unlock()
	if call.Error == nil {
		call.Error = err
	}
}

func (call *Call) getError() error {
	call.errorMu.Lock()
	defer call.errorMu.Unlock()
	return call.Error
}
