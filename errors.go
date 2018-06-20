package rpc

import "errors"

// ResponseErr is an enum type for providing error type
// information over the wire between rpc server and client.
type ResponseErr int

const (
	// NonRPCErr is an error that hasn't arisen from the gorpc package.
	NonRPCErr ResponseErr = iota
	// ServerErr is an error that has arisen on the server side.
	ServerErr
	// ClientErr is an error that has arisen on the client side.
	ClientErr
)

// ServerError indicates that error originated in server
// specific code.
type ServerError struct {
	msg string
}

func (s *ServerError) Error() string {
	return s.msg
}

// NewServerError wraps an error in the ServerError type.
func NewServerError(err error) error {
	return &ServerError{err.Error()}
}

// ClientError indicates that error originated in client
// specific code.
type ClientError struct {
	msg string
}

func (c *ClientError) Error() string {
	return c.msg
}

// NewClientError wraps an error in the ClientError type.
func NewClientError(err error) error {
	return &ClientError{err.Error()}
}

// ResponseError converts an ResponseErr and error message string
// into the appropriate error type.
func ResponseError(errType ResponseErr, errMsg string) error {
	switch errType {
	case ServerErr:
		return &ServerError{errMsg}
	case ClientErr:
		return &ClientError{errMsg}
	default:
		return errors.New(errMsg)
	}
}

// ResponseErrorType determines whether an error is of either
// ServerError or ClientError type and returns the appropriate
// ResponseErr value.
func ResponseErrorType(err error) ResponseErr {
	switch err.(type) {
	case *ServerError:
		return ServerErr
	case *ClientError:
		return ClientErr
	default:
		return NonRPCErr
	}
}

// IsRPCError returns whether an error is either a ServerError
// or ClientError.
func IsRPCError(err error) bool {
	switch err.(type) {
	case *ServerError, *ClientError:
		return true
	default:
		return false
	}
}
