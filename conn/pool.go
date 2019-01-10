/*
 * Copyright 2016-2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package conn

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/dgo/protos/api"
	"github.com/dgraph-io/dgraph/protos/pb"
	"github.com/dgraph-io/dgraph/x"
	"github.com/golang/glog"
	"go.opencensus.io/plugin/ocgrpc"

	"google.golang.org/grpc"
)

var (
	ErrNoConnection        = fmt.Errorf("No connection exists")
	ErrUnhealthyConnection = fmt.Errorf("Unhealthy connection")
	errNoPeerPoolEntry     = fmt.Errorf("no peerPool entry")
	errNoPeerPool          = fmt.Errorf("no peerPool pool, could not connect")
	echoDuration           = time.Second
)

// 单个pool结构体
// "Pool" is used to manage the grpc client connection(s) for communicating with other
// worker instances.  Right now it just holds one of them.
type Pool struct {
	sync.RWMutex

	// A "pool" now consists of one connection.  gRPC uses HTTP2 transport to combine
	// messages in the same TCP stream.
	conn *grpc.ClientConn // grpc 客户端连接

	// 最后使用时间 , 可用于判断健康状态
	lastEcho time.Time

	// 当前连接对应的addr , 实际值为 ip+port
	Addr string

	//实际为echoDuration  , 每隔1s检测是否存活
	ticker *time.Ticker
}

type Pools struct {
	sync.RWMutex
	// 使用map管理连接池 , 其中key为addr (ip+port)
	all map[string]*Pool
}

// 内部全局变量
var pi *Pools

// 初始化连接池内部全局变量
func init() {
	pi = new(Pools)
	pi.all = make(map[string]*Pool)
}

// 提供给外部获取连接池方法
func Get() *Pools {
	return pi
}

func (p *Pools) Get(addr string) (*Pool, error) {
	p.RLock()
	defer p.RUnlock()
	pool, ok := p.all[addr]
	if !ok {
		return nil, ErrNoConnection // 说明没有建立连接
	}

	// 检测取到的pool是否健康, 如果不健康返回错误
	if !pool.IsHealthy() {
		return nil, ErrUnhealthyConnection
	}
	return pool, nil
}

// 根据在线alpha成员在线状态, 移除掉不在线的服务器
func (p *Pools) RemoveInvalid(state *pb.MembershipState) {
	// Keeps track of valid IP addresses, assigned to active nodes. We do this
	// to avoid removing valid IP addresses from the Removed list.
	validAddr := make(map[string]struct{})
	for _, group := range state.Groups {
		for _, member := range group.Members {
			validAddr[member.Addr] = struct{}{}
		}
	}
	for _, member := range state.Zeros {
		validAddr[member.Addr] = struct{}{}
	}
	// 移除的时候 , 需要避免掉目前还在线提供服务的addr 不能被删除
	for _, member := range state.Removed {
		// Some nodes could have the same IP address. So, check before disconnecting.
		if _, valid := validAddr[member.Addr]; !valid {
			p.remove(member.Addr)
		}
	}
}

// 从p.all 这个map中移除掉addr
func (p *Pools) remove(addr string) {
	p.Lock()
	pool, ok := p.all[addr]
	if !ok { // 要移除的连接不在池子中
		p.Unlock()
		return
	}
	glog.Warningf("DISCONNECTING from %s\n", addr)
	delete(p.all, addr)
	p.Unlock()
	pool.shutdown() // 关闭这个pool
}

// 直接获取连接, 如果没有新建一个, 并放入到连接池中
func (p *Pools) Connect(addr string) *Pool {
	p.RLock()
	existingPool, has := p.all[addr]
	if has { // 如果池子中已经存在, 不需要在创建连接
		p.RUnlock()
		return existingPool
	}
	p.RUnlock()

	pool, err := NewPool(addr) // 创建一个pool出来
	if err != nil {
		glog.Errorf("Unable to connect to host: %s", addr)
		return nil
	}

	p.Lock()                        // 写锁
	existingPool, has = p.all[addr] //再次检查是否存在, 因为别的协程有可能已经把连接放到池子中
	if has {
		p.Unlock()
		return existingPool
	}
	glog.Infof("CONNECTED to %v\n", addr)
	p.all[addr] = pool
	p.Unlock()
	return pool
}

// NewPool creates a new "pool" with one gRPC connection, refcount 0.
func NewPool(addr string) (*Pool, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithStatsHandler(&ocgrpc.ClientHandler{}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(x.GrpcMaxSize),
			grpc.MaxCallSendMsgSize(x.GrpcMaxSize)),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	pl := &Pool{conn: conn, Addr: addr, lastEcho: time.Now()}

	// 更新raft健康状态
	pl.UpdateHealthStatus(true)

	// Initialize ticker before running monitor health.
	pl.ticker = time.NewTicker(echoDuration)
	go pl.MonitorHealth() // 启动一个协程, 每秒检查raft状态
	return pl, nil
}

// Get returns the connection to use from the pool of connections.
func (p *Pool) Get() *grpc.ClientConn {
	p.RLock()
	defer p.RUnlock()
	return p.conn
}

func (p *Pool) shutdown() {
	p.ticker.Stop()
	p.conn.Close()
}

func (p *Pool) SetUnhealthy() {
	p.Lock()
	p.lastEcho = time.Time{}
	p.Unlock()
}

func (p *Pool) UpdateHealthStatus(printError bool) error {
	conn := p.Get()

	query := new(api.Payload)
	query.Data = make([]byte, 10)
	x.Check2(rand.Read(query.Data))

	// 创建raft客户端
	c := pb.NewRaftClient(conn)
	// Ensure that we have a timeout here, otherwise a network partition could
	// end up causing this RPC to get stuck forever.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.Echo(ctx, query) // 1秒后关闭
	if err == nil {                 // 说明更新raft 成功
		x.AssertTruef(bytes.Equal(resp.Data, query.Data),
			"non-matching Echo response value from %v", p.Addr)
		p.Lock()
		p.lastEcho = time.Now() // 设置最后更新状态
		p.Unlock()
	} else if printError { // 说明raft失败了
		glog.Errorf("Echo error from %v. Err: %v\n", p.Addr, err)
	}
	return err
}

// MonitorHealth monitors the health of the connection via Echo. This function blocks forever.
func (p *Pool) MonitorHealth() {
	var lastErr error
	for range p.ticker.C { // 每秒检查更新状态
		err := p.UpdateHealthStatus(lastErr == nil) // 当上次没有出错的时候, 且本次同步raft出错了,打印日志
		if lastErr != nil && err == nil {           //只有上次出错了, 本次更新成功的时候, 才打印建立连接成功
			glog.Infof("Connection established with %v\n", p.Addr)
		}
		lastErr = err
	}
}

// 判断是否健康
func (p *Pool) IsHealthy() bool {
	if p == nil {
		return false
	}
	p.RLock()
	defer p.RUnlock()
	return time.Since(p.lastEcho) < 2*echoDuration //当前时间距离最后一次更新的时间, 如果超过2s , 返回不健康
}
