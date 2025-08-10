package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
	"time"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	return &Clerk{clnt: clnt, servers: servers, leader: 0}
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	n := len(ck.servers)
	start := ck.leader
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			var reply rpc.GetReply
			if ck.clnt.Call(ck.servers[idx], "KVServer.Get", &args, &reply) {
				if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup {
					ck.leader = idx
					return reply.Value, reply.Version, reply.Err
				}
			}
		}
		// brief backoff to avoid hot loop under losses
		time.Sleep(10 * time.Millisecond)
	}
	return "", 0, rpc.ErrWrongLeader
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	n := len(ck.servers)
	start := ck.leader
	firstRound := true
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			var reply rpc.PutReply
			if ck.clnt.Call(ck.servers[idx], "KVServer.Put", &args, &reply) {
				if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup {
					ck.leader = idx
					return reply.Err
				}
				if reply.Err == rpc.ErrVersion {
					if firstRound {
						return rpc.ErrVersion
					}
					return rpc.ErrMaybe
				}
			}
		}
		firstRound = false
		time.Sleep(10 * time.Millisecond)
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	var reply shardrpc.FreezeShardReply
	n := len(ck.servers)
	start := ck.leader
	sawLeaderErr := false
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if ck.clnt.Call(ck.servers[idx], "KVServer.FreezeShard", &args, &reply) {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
				ck.leader = idx
				return reply.State, reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				sawLeaderErr = true
				continue
			}
		}
	}
	if sawLeaderErr {
		return nil, rpc.ErrWrongLeader
	}
	// treat as no contact with any server in group this round
	return nil, rpc.ErrWrongLeader
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	var reply shardrpc.InstallShardReply
	n := len(ck.servers)
	start := ck.leader
	sawLeaderErr := false
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if ck.clnt.Call(ck.servers[idx], "KVServer.InstallShard", &args, &reply) {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
				ck.leader = idx
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				sawLeaderErr = true
				continue
			}
		}
	}
	if sawLeaderErr {
		return rpc.ErrWrongLeader
	}
	return rpc.ErrWrongLeader
}
func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	var reply shardrpc.DeleteShardReply
	n := len(ck.servers)
	start := ck.leader
	sawLeaderErr := false
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if ck.clnt.Call(ck.servers[idx], "KVServer.DeleteShard", &args, &reply) {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
				ck.leader = idx
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				sawLeaderErr = true
				continue
			}
		}
	}
	if sawLeaderErr {
		return rpc.ErrWrongLeader
	}
	return rpc.ErrWrongLeader
}
