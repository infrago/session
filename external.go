package session

import (
	"time"

	. "github.com/infrago/base"
)

// Read 读取会话
func Read(id string) (Map, error) {
	return module.Read(id)
}

// Write 写会话
func Write(id string, value Map, expires ...time.Duration) error {
	return module.Write(id, value, expires...)
}

// Delete 删除会话
func Delete(id string) error {
	return module.Delete(id)
}

// Exists 是否存在会话
func Exists(id string) (bool, error) {
	return module.Exists(id)
}

// KeysFrom 获取Keys
func Keys(prefixs ...string) ([]string, error) {
	return module.Keys(prefixs...)
}

// Clear 清理会话
func Clear(prefixs ...string) error {
	return module.Clear(prefixs...)
}
