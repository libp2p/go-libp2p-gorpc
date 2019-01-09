package stats

import "time"

// RPCStats contains stats information about RPCs.
type RPCStats interface {
	isRPCStats()
	// IsClient returns true if this RPCStats is from client side.
	IsClient() bool
}

// Begin contains stats when an RPC begins.
// FailFast is only valid if this Begin is from client side.
type Begin struct {
	// Client is true if this Begin is from client side.
	Client bool
	// BeginTime is the time when the RPC begins.
	BeginTime time.Time
}

// IsClient indicates if the stats information is from client side.
func (s *Begin) IsClient() bool { return s.Client }

func (s *Begin) isRPCStats() {}

// InPayload contains the information for an incoming payload.
type InPayload struct {
	// Client is true if this InPayload is from client side.
	Client bool
	// Payload is the payload with original type.
	Payload interface{}
	// Data is the serialized message payload.
	Data []byte
	// Length is the length of uncompressed data.
	Length int
	// WireLength is the length of data on wire (compressed, signed, encrypted).
	WireLength int
	// RecvTime is the time when the payload is received.
	RecvTime time.Time
}

// IsClient indicates if the stats information is from client side.
func (s *InPayload) IsClient() bool { return s.Client }

func (s *InPayload) isRPCStats() {}

// OutPayload contains the information for an outgoing payload.
type OutPayload struct {
	// Client is true if this OutPayload is from client side.
	Client bool
	// Payload is the payload with original type.
	Payload interface{}
	// Data is the serialized message payload.
	Data []byte
	// Length is the length of uncompressed data.
	Length int
	// WireLength is the length of data on wire (compressed, signed, encrypted).
	WireLength int
	// SentTime is the time when the payload is sent.
	SentTime time.Time
}

// IsClient indicates if this stats information is from client side.
func (s *OutPayload) IsClient() bool { return s.Client }

func (s *OutPayload) isRPCStats() {}

// End contains stats when an RPC ends.
type End struct {
	// Client is true if this End is from client side.
	Client bool
	// BeginTime is the time when the RPC began.
	BeginTime time.Time
	// EndTime is the time when the RPC ends.
	EndTime time.Time
	// Error is the error the RPC ended with. It is an error generated from
	// status.Status and can be converted back to status.Status using
	// status.FromError if non-nil.
	Error error
}

// IsClient indicates if this is from client side.
func (s *End) IsClient() bool { return s.Client }

func (s *End) isRPCStats() {}
