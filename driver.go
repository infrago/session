package session

import (
	"time"
)

type (
	// Driver 数据驱动
	Driver interface {
		Connect(*Instance) (Connect, error)
	}

	// Connect 会话连接
	Connect interface {
		Open() error
		Close() error

		Read(string) ([]byte, error)
		Write(id string, val []byte, expiry time.Duration) error
		Exists(id string) (bool, error)
		Delete(id string) error
		Keys() ([]string, error)
		Clear() error
	}
)
