package rpc

import "errors"

// responseErr is an enum type for providing error type
// information over the wire between rpc server and client.
type responseErr int

const (
	// nonRPCErr is an error that hasn't arisen from the gorpc package.
	nonRPCErr responseErr = iota
	// serverErr is an error that has arisen on the server side.
	serverErr
	// clientErr is an error that has arisen on the client side.
	clientErr
	// authErr is an error that has arisen because client doesn't
	// have permissions to make the given rpc request
	authErr
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

// authError indicates that error originated because of client not having
// permissions to make given rpc request
type authError struct {
	msg string
}

func (a *authError) Error() string {
	return a.msg
}

// newAuthError wraps an error in the authError type.
func newAuthError(err error) error {
	return &authError{err.Error()}
}

// responseError converts an responseErr and error message string
// into the appropriate error type.
func responseError(errType responseErr, errMsg string) error {
	switch errType {
	case serverErr:
		return &serverError{errMsg}
	case clientErr:
		return &clientError{errMsg}
	case authErr:
		return &authError{errMsg}
	default:
		return errors.New(errMsg)
	}
}

// responseErrorType determines whether an error is of either
// serverError or clientError type and returns the appropriate
// responseErr value.
func responseErrorType(err error) responseErr {
	switch err.(type) {
	case *serverError:
		return serverErr
	case *clientError:
		return clientErr
	case *authError:
		return authErr
	default:
		return nonRPCErr
	}
}

// IsRPCError returns whether an error is either a serverError
// or clientError.
func IsRPCError(err error) bool {
	switch err.(type) {
	case *serverError, *clientError, *authError:
		return true
	default:
		return false
	}
}

// IsServerError returns whether an error is serverError.
func IsServerError(err error) bool {
	return responseErrorType(err) == serverErr
}

// IsClientError returns whether an error is clientError.
func IsClientError(err error) bool {
	return responseErrorType(err) == clientErr
}

// IsAuthError returns whether an error is authError.
func IsAuthError(err error) bool {
	return responseErrorType(err) == authErr
}
