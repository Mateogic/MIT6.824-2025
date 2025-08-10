package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const debugGrp = false
func gdlogf(format string, args ...any) {
	if debugGrp {
		log.Printf(format, args...)
	}
}

type KVServer struct {
	me   int
	gid  tester.Tgid
	rsm  *rsm.RSM
	dead int32 // set by Kill()

	mu     sync.Mutex
	kv     map[string]string
	vers   map[string]rpc.Tversion
	owned  map[shardcfg.Tshid]bool
	frozen map[shardcfg.Tshid]bool
	maxNum map[shardcfg.Tshid]shardcfg.Tnum
	// remember if we already installed a shard for a config to avoid repeated decoding
	installed map[shardcfg.Tshid]shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	switch v := req.(type) {
	case rpc.GetArgs:
		sh := shardcfg.Key2Shard(v.Key)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if !kv.owned[sh] {
			gdlogf("[grp %d:%d] GET key=%s shard=%d -> ErrWrongGroup (owned=%t frozen=%t maxNum=%d)", kv.gid, kv.me, v.Key, sh, kv.owned[sh], kv.frozen[sh], kv.maxNum[sh])
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		if val, ok := kv.kv[v.Key]; ok {
			return rpc.GetReply{Value: val, Version: kv.vers[v.Key], Err: rpc.OK}
		}
		return rpc.GetReply{Err: rpc.ErrNoKey}
	case rpc.PutArgs:
		sh := shardcfg.Key2Shard(v.Key)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if !kv.owned[sh] || kv.frozen[sh] {
			gdlogf("[grp %d:%d] PUT key=%s shard=%d -> ErrWrongGroup (owned=%t frozen=%t maxNum=%d)", kv.gid, kv.me, v.Key, sh, kv.owned[sh], kv.frozen[sh], kv.maxNum[sh])
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		curVer, exists := kv.vers[v.Key]
		if !exists {
			if v.Version == 0 {
				kv.kv[v.Key] = v.Value
				kv.vers[v.Key] = 1
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		if v.Version == curVer {
			kv.kv[v.Key] = v.Value
			kv.vers[v.Key] = curVer + 1
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrVersion}
	case shardrpc.FreezeShardArgs:
		s := v.Shard
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if v.Num < kv.maxNum[s] {
			gdlogf("[grp %d:%d] FREEZE shard=%d num=%d < max=%d -> ErrVersion", kv.gid, kv.me, s, v.Num, kv.maxNum[s])
			return shardrpc.FreezeShardReply{State: nil, Num: kv.maxNum[s], Err: rpc.ErrVersion}
		}
		kv.maxNum[s] = v.Num
		if !kv.owned[s] {
			gdlogf("[grp %d:%d] FREEZE shard=%d not-owned -> OK(empty)", kv.gid, kv.me, s)
			return shardrpc.FreezeShardReply{State: []byte{}, Num: v.Num, Err: rpc.OK}
		}
		gdlogf("[grp %d:%d] FREEZE shard=%d -> frozen", kv.gid, kv.me, s)
		kv.frozen[s] = true
		m1 := make(map[string]string)
		m2 := make(map[string]rpc.Tversion)
		for k, val := range kv.kv {
			if shardcfg.Key2Shard(k) == s {
				m1[k] = val
				m2[k] = kv.vers[k]
			}
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		_ = e.Encode(m1)
		_ = e.Encode(m2)
		return shardrpc.FreezeShardReply{State: w.Bytes(), Num: v.Num, Err: rpc.OK}
	case shardrpc.InstallShardArgs:
		s := v.Shard
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if v.Num < kv.maxNum[s] {
			gdlogf("[grp %d:%d] INSTALL shard=%d num=%d < max=%d -> ErrVersion", kv.gid, kv.me, s, v.Num, kv.maxNum[s])
			return shardrpc.InstallShardReply{Err: rpc.ErrVersion}
		}
			kv.maxNum[s] = v.Num
			if n, ok := kv.installed[s]; ok && n >= v.Num {
				// already installed this or newer
				kv.owned[s] = true
				kv.frozen[s] = false
			gdlogf("[grp %d:%d] INSTALL shard=%d num=%d already installed (max=%d)", kv.gid, kv.me, s, v.Num, kv.maxNum[s])
				return shardrpc.InstallShardReply{Err: rpc.OK}
			}
		if len(v.State) > 0 {
			r := bytes.NewBuffer(v.State)
			d := labgob.NewDecoder(r)
			m1 := make(map[string]string)
			m2 := make(map[string]rpc.Tversion)
			if d.Decode(&m1) == nil && d.Decode(&m2) == nil {
				for k, val := range m1 {
					if shardcfg.Key2Shard(k) == s {
						kv.kv[k] = val
						kv.vers[k] = m2[k]
					}
				}
			}
		}
		kv.owned[s] = true
		kv.frozen[s] = false
			kv.installed[s] = v.Num
		gdlogf("[grp %d:%d] INSTALL shard=%d num=%d done (owned=%t)", kv.gid, kv.me, s, v.Num, kv.owned[s])
		return shardrpc.InstallShardReply{Err: rpc.OK}
	case shardrpc.DeleteShardArgs:
		s := v.Shard
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if v.Num < kv.maxNum[s] {
			gdlogf("[grp %d:%d] DELETE shard=%d num=%d < max=%d -> OK(idemp)", kv.gid, kv.me, s, v.Num, kv.maxNum[s])
			return shardrpc.DeleteShardReply{Err: rpc.OK}
		}
		kv.maxNum[s] = v.Num
		for k := range kv.kv {
			if shardcfg.Key2Shard(k) == s {
				delete(kv.kv, k)
				delete(kv.vers, k)
			}
		}
		kv.owned[s] = false
		kv.frozen[s] = false
		delete(kv.installed, s)
		gdlogf("[grp %d:%d] DELETE shard=%d num=%d done", kv.gid, kv.me, s, v.Num)
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.kv)
	_ = e.Encode(kv.vers)
	_ = e.Encode(kv.owned)
	_ = e.Encode(kv.frozen)
	_ = e.Encode(kv.maxNum)
	_ = e.Encode(kv.installed)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	m1 := make(map[string]string)
	m2 := make(map[string]rpc.Tversion)
	m3 := make(map[shardcfg.Tshid]bool)
	m4 := make(map[shardcfg.Tshid]bool)
	m5 := make(map[shardcfg.Tshid]shardcfg.Tnum)
	m6 := make(map[shardcfg.Tshid]shardcfg.Tnum)
	if d.Decode(&m1) == nil && d.Decode(&m2) == nil && d.Decode(&m3) == nil && d.Decode(&m4) == nil && d.Decode(&m5) == nil && d.Decode(&m6) == nil {
		kv.mu.Lock()
		kv.kv = m1
		kv.vers = m2
		kv.owned = m3
		kv.frozen = m4
		kv.maxNum = m5
		kv.installed = m6
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	if atomic.LoadInt32(&kv.dead) == 1 {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	if atomic.LoadInt32(&kv.dead) == 1 {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	if atomic.LoadInt32(&kv.dead) == 1 {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	if atomic.LoadInt32(&kv.dead) == 1 {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	if atomic.LoadInt32(&kv.dead) == 1 {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// StartServerShardGrp starts a server for shard group `gid` and returns services to register.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// labgob registrations
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:   gid,
		me:    me,
		kv:    make(map[string]string),
		vers:  make(map[string]rpc.Tversion),
		owned: make(map[shardcfg.Tshid]bool),
		frozen: make(map[shardcfg.Tshid]bool),
		maxNum: make(map[shardcfg.Tshid]shardcfg.Tnum),
		installed: make(map[shardcfg.Tshid]shardcfg.Tnum),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// initial ownership: Gid1 starts with all shards
	if gid == shardcfg.Gid1 {
		for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
			kv.owned[s] = true
			kv.maxNum[s] = shardcfg.NumFirst
		}
	}
	return []tester.IService{kv, kv.rsm.Raft()}
}
