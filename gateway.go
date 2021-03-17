// gateway.go 是一个网关
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/xhyonline/xutil/xlog"
	"sync"
)

var log = xlog.Get(true)

// Node 真正提供服务的
type Node struct {
	// 该节点的主机地址
	Host string `json:"host"`
	// 端口
	Port string `json:"port"`
}

// Config etcd 配置信息
type Config struct {
	Node
}

// ServiceDiscovery 是一个服务发现抽象
type ServiceDiscovery struct {
	// etcd 客户端
	client *clientv3.Client
	// etcd 的 key 的前缀
	prefix string
	// 各服务节点, Key 为服务名
	nodes map[string]*Node
	// 保证 nodes 并发安全
	lock sync.RWMutex
}

// NewServiceDiscovery 实例化一个服务发现实例
func NewServiceDiscovery(c Config, prefix string) *ServiceDiscovery {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{c.Host + ":" + c.Port},
	})

	if err != nil {
		panic(err)
	}
	return &ServiceDiscovery{
		client: cli,
		prefix: prefix,
		nodes:  make(map[string]*Node),
		lock:   sync.RWMutex{},
	}
}

// Watch 监听事件
func (s *ServiceDiscovery) Watch() error {
	// 先获取先前前缀下的所有服务
	kv := clientv3.KV(s.client)
	getResp, err := kv.Get(context.Background(), s.prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	s.lock.Lock()
	// 将所有服务全部都扔进集合中
	for _, item := range getResp.Kvs {
		err = s.addService(item.Key, item.Value)
		if err != nil {
			return err
		}
	}
	s.lock.Unlock()

	// 开始正式监听

	watcher := clientv3.NewWatcher(s.client)

	defer watcher.Close()
	// 从 getResp.Header.Revision+1 开始,监听后续所有的以 s.prefix 前缀开头的 key 事件
	c := watcher.Watch(context.Background(), s.prefix, clientv3.WithPrefix(), clientv3.WithRev(getResp.Header.Revision+1))
	// 从管道持续读取
	for watchResp := range c {
		for _, event := range watchResp.Events {
			s.lock.Lock()
			switch event.Type {
			case mvccpb.PUT:
				fmt.Println("写入事件触发,写入的 key 是:" + string(event.Kv.Key) + "值是:" + string(event.Kv.Value))
				if err = s.addService(event.Kv.Key, event.Kv.Value); err != nil {
					log.Errorf("不期待的数据结构,key 为:", string(event.Kv.Key), "值为", string(event.Kv.Value))
					s.lock.Unlock()
					// 跳过这个
					continue
				}
			case mvccpb.DELETE:
				fmt.Println("删除事件触发,删除的 key 是:" + string(event.Kv.Key))
				s.removeService(event.Kv.Key)
			}
			s.lock.Unlock()
			s.show()
		}
	}
	return nil
}

// addService 新增服务
func (s *ServiceDiscovery) addService(key, value []byte) error {
	node := new(Node)
	if err := json.Unmarshal(value, node); err != nil {
		return err
	}
	s.nodes[string(key)] = node
	return nil
}

// removeService 删除服务
func (s *ServiceDiscovery) removeService(key []byte) {
	delete(s.nodes, string(key))
}

// show 展示当前的服务
func (s *ServiceDiscovery) show() {
	s.lock.RLock()
	for k, v := range s.nodes {
		fmt.Println("服务"+k+"IP为:"+v.Host+"端口为:", v.Port)
	}
	s.lock.RUnlock()
}

func main() {
	s := NewServiceDiscovery(Config{Node{
		Host: "121.5.62.93",
		Port: "2379",
	}}, "/discovery/")
	err := s.Watch()
	if err != nil {
		panic(err)
	}
}
