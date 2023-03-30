package session

import "errors"

const (
	NAME = "SESSION"
)

var (
	ErrInvalidConnection = errors.New("Invalid session connection.")
)
