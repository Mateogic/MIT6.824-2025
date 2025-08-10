package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
	"log"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
	cfg  *shardcfg.ShardConfig
}

const debugCli = false
func cdlogf(format string, args ...any) {
	if debugCli {
		log.Printf(format, args...)
	}
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.
	ck.cfg = sck.Query()
	return ck
}


// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		if ck.cfg == nil {
			ck.cfg = ck.sck.Query()
		}
		sh := shardcfg.Key2Shard(key)
		gid, srvs, ok := ck.cfg.GidServers(sh)
		if ok {
			gck := shardgrp.MakeClerk(ck.clnt, srvs)
			cdlogf("[cli] GET key=%s shard=%d gid=%d cfg=%d", key, sh, gid, ck.cfg.Num)
			v, ver, err := gck.Get(key)
			if err == rpc.OK || err == rpc.ErrNoKey {
				return v, ver, err
			}
		}
		// refresh config and retry
		cdlogf("[cli] GET key=%s shard=%d -> refresh cfg (prev=%d)", key, sh, ck.cfg.Num)
		ck.cfg = ck.sck.Query()
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	first := true
	for {
		if ck.cfg == nil {
			ck.cfg = ck.sck.Query()
		}
		sh := shardcfg.Key2Shard(key)
		gid, srvs, ok := ck.cfg.GidServers(sh)
		if ok {
			gck := shardgrp.MakeClerk(ck.clnt, srvs)
			cdlogf("[cli] PUT key=%s shard=%d gid=%d cfg=%d ver=%d", key, sh, gid, ck.cfg.Num, version)
			err := gck.Put(key, value, version)
			if err == rpc.OK || err == rpc.ErrNoKey {
				return err
			}
			if err == rpc.ErrVersion {
				if first {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		}
		first = false
		cdlogf("[cli] PUT key=%s shard=%d -> refresh cfg (prev=%d)", key, sh, ck.cfg.Num)
		ck.cfg = ck.sck.Query()
	}
}
