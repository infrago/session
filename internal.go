package session

import (
	"strings"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

func (this *Module) getInst(id string) (*Instance, error) {
	conn := this.hashring.Locate(id)
	if inst, ok := this.instances[conn]; ok {
		return inst, nil
	}
	return nil, ErrInvalidConnection
}

// Exists
// 判断会话是否存在
func (this *Module) Exists(id string) (bool, error) {
	inst, err := this.getInst(id)
	if err != nil {
		return false, err
	}
	return inst.connect.Exists(id)
}

// Read 读取会话
func (this *Module) Read(id string) (Map, error) {
	inst, err := this.getInst(id)
	if err != nil {
		return nil, err
	}

	//加前缀
	realid := inst.Config.Prefix + id
	data, err := inst.connect.Read(realid)
	if err != nil {
		return nil, err
	}

	val := Map{}
	err = infra.Unmarshal(inst.Config.Codec, data, &val)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Write 写会话
func (this *Module) Write(id string, val Map, expiries ...time.Duration) error {
	inst, err := this.getInst(id)
	if err != nil {
		return err
	}

	//默认超时时间
	expiry := inst.Config.Expiry
	if len(expiries) > 0 {
		expiry = expiries[0]
	}

	// 编码数据
	data, err := infra.Marshal(inst.Config.Codec, &val)
	if err != nil {
		return err
	}

	//KEY加上前缀
	realid := inst.Config.Prefix + id
	return inst.connect.Write(realid, data, expiry)
}

// Delete 删除会话
func (this *Module) Delete(id string) error {
	inst, err := this.getInst(id)
	if err != nil {
		return err
	}

	realKey := inst.Config.Prefix + id
	return inst.connect.Delete(realKey)
}

// Keys 获取所有前缀的KEYS
func (this *Module) Keys(prefixs ...string) ([]string, error) {
	prefix := ""
	if len(prefixs) > 0 {
		prefix = prefixs[0]
	}

	keys := make([]string, 0)

	//全部库
	for _, inst := range this.instances {
		realPrefix := inst.Config.Prefix + prefix
		temps, err := inst.connect.Keys(realPrefix)
		if err == nil {
			for _, temp := range temps {
				keys = append(keys, strings.TrimPrefix(temp, realPrefix))
			}
		}
	}
	return keys, nil
}

// Clear 按前缀清理缓存
func (this *Module) Clear(prefixs ...string) error {
	prefix := ""
	if len(prefixs) > 0 {
		prefix = prefixs[0]
	}

	for _, inst := range this.instances {
		realPrefix := inst.Config.Prefix + prefix
		inst.connect.Clear(realPrefix)
	}
	return nil
}
