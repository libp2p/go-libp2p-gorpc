package rpc

import "errors"

// ErrorType is an enum type for providing error type
// information over the wire between rpc server and client.
type ErrorType int

const (
	// NonRPCErr is an error that hasn't arisen from the gorpc package.
	NonRPCErr ErrorType = iota
	// ServerErr is an error that has arisen on the server side.
	ServerErr
	// ClientErr is an error that has arisen on the client side.
	ClientErr
	// AuthorizationErr is an error that has arisen because client doesn't
	// have permissions to make the given rpc request
	AuthorizationErr
)

// serverError indicates that error originated in server
// specific code.
type serverError struct {
	msg string
}

func (s *serverError) Error() string {
	return s.msg
}

// newServerError wraps an error in the serverError type.
func newServerError(err error) error {
	return &serverError{err.Error()}
}

// clientError indicates that error originated in client
// specific code.
type clientError struct {
	msg string
}

func (c *clientError) Error() string {
	return c.msg
}

// newClientError wraps an error in the clientError type.
func newClientError(err error) error {
	return &clientError{err.Error()}
}

// authorizationError indicates that error originated because of client not having
// permissions to make given rpc request
type authorizationError struct {
	msg string
}

func (a *authorizationError) Error() string {
	return a.msg
}

// newAuthorizationError wraps an error in the authorizationError type.
func newAuthorizationError(err error) error {
	return &authorizationError{err.Error()}
}

// responseError converts an responseErr and error message string
// into the appropriate error type.
func responseError(errType ErrorType, errMsg string) error {
	switch errType {
	case ServerErr:
		return &serverError{errMsg}
	case ClientErr:
		return &clientError{errMsg}
	case AuthorizationErr:
		return &authorizationError{errMsg}
	default:
		return errors.New(errMsg)
	}
}

// responseErrorType determines whether an error is of either
// serverError or clientError type and returns the appropriate
// responseErr value.
func responseErrorType(err error) ErrorType {
	switch err.(type) {
	case *serverError:
		return ServerErr
	case *clientError:
		return ClientErr
	case *authorizationError:
		return AuthorizationErr
	default:
		return NonRPCErr
	}
}

// IsRPCError returns whether an error is either a serverError
// or clientError.
func IsRPCError(err error) bool {
	switch err.(type) {
	case *serverError, *clientError, *authorizationError:
		return true
	default:
		return false
	}
}

// IsServerError returns whether an error is serverError.
func IsServerError(err error) bool {
	return responseErrorType(err) == ServerErr
}

// IsClientError returns whether an error is clientError.
func IsClientError(err error) bool {
	return responseErrorType(err) == ClientErr
}

// IsAuthorizationError returns whether an error is authorizationError.
func IsAuthorizationError(err error) bool {
	return responseErrorType(err) == AuthorizationErr
}
