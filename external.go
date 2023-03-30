package session

import (
	"time"

	. "github.com/infrago/base"
)

// Read 读取会话
func Read(key string) (Map, error) {
	return module.Read(key)
}

// ReadFrom 指定库读取会话
func ReadFrom(conn, key string) (Map, error) {
	return module.ReadFrom(conn, key)
}

// ReadData 读取原始数据
func ReadData(key string) ([]byte, error) {
	return module.ReadData(key)
}

// ReadDataFrom 指定库读取原始数据
func ReadDataFrom(conn, key string) ([]byte, error) {
	return module.ReadDataFrom(conn, key)
}

// Write 写会话
func Write(key string, value Map, expiries ...time.Duration) error {
	return module.Write(key, value, expiries...)
}

// WriteTo 指定库写会话
func WriteTo(conn, key string, value Map, expiries ...time.Duration) error {
	return module.WriteTo(conn, key, value, expiries...)
}

// WriteData 写会话原始数据
func WriteData(key string, data []byte, expiries ...time.Duration) error {
	return module.WriteData(key, data, expiries...)
}

// WriteDataTo 指定库写会话原始数据
func WriteDataTo(conn, key string, data []byte, expiries ...time.Duration) error {
	return module.WriteDataTo(conn, key, data, expiries...)
}

// Delete 删除会话
func Delete(key string) error {
	return module.Delete(key)
}

// DeleteFrom 指定库删除会话
func DeleteFrom(conn, key string) error {
	return module.DeleteFrom(conn, key)
}

// Exists 是否存在会话
func Exists(key string) (bool, error) {
	return module.Exists(key)
}

// ExistsIn 指定库是否存在会话
func ExistsIn(conn, key string) (bool, error) {
	return module.ExistsIn(conn, key)
}

// Sequence 生成序列
func Sequence(key string, start, step int64, expiries ...time.Duration) (int64, error) {
	return module.Sequence(key, start, step, expiries...)
}

// SequenceOn 指定库生成序列
func SequenceOn(conn, key string, start, step int64, expiries ...time.Duration) (int64, error) {
	return module.SequenceOn(conn, key, start, step, expiries...)
}

// KeysFrom 获取Keys
func Keys(prefixs ...string) ([]string, error) {
	return module.Keys(prefixs...)
}

// KeysFrom 从指定库获取Keys
func KeysFrom(conn string, prefixs ...string) ([]string, error) {
	return module.KeysFrom(conn, prefixs...)
}

// Clear 清理会话
func Clear(prefixs ...string) error {
	return module.Clear(prefixs...)
}

// ClearFrom 从指定库清除会话
func ClearFrom(conn string, prefixs ...string) error {
	return module.ClearFrom(conn, prefixs...)
}
