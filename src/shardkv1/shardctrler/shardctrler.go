package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

// toggle controller debug logging
const debugSck = false

func dlogf(format string, args ...any) {
	if debugSck {
		log.Printf(format, args...)
	}
}

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	// Recovery for B/C: if there's a pending next-config, finish it.
	const cfgCurKey = "shardcfg/current"
	const cfgNextKey = "shardcfg/next"

	// ensure client exists
	if sck.IKVClerk == nil {
		log.Fatalf("InitController: IKVClerk is nil")
	}
	// read current
	curVal, _, err := sck.IKVClerk.Get(cfgCurKey)
	var cur *shardcfg.ShardConfig
	if err == rpc.ErrNoKey {
		cur = shardcfg.MakeShardConfig()
	} else {
		cur = shardcfg.FromString(curVal)
	}
	// read next; if absent, nothing to recover
	nextVal, _, err := sck.IKVClerk.Get(cfgNextKey)
	if err == rpc.ErrNoKey {
		return
	}
	next := shardcfg.FromString(nextVal)
	// if next already applied or stale, nothing to do
	if next.Num <= cur.Num {
		return
	}
	// finish migration to next
	sck.ChangeConfigTo(next)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	if sck.IKVClerk == nil {
		log.Fatalf("InitConfig: IKVClerk is nil")
	}
	// choose a fixed key to store the current configuration
	const cfgCurKey = "shardcfg/current"
	// store initial config at version 0
	_ = sck.IKVClerk.Put(cfgCurKey, cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Part A: sequential reconfiguration without failures.
	cur := sck.Query()
	if new.Num <= cur.Num {
		return
	}
	dlogf("[sck] ChangeConfigTo: cur=%d -> new=%d", cur.Num, new.Num)
	// Persist desired next configuration so a new controller can recover.
	const cfgNextKey = "shardcfg/next"
	if sck.IKVClerk == nil {
		log.Fatalf("ChangeConfigTo: IKVClerk is nil")
	}
	// try to upsert next: read current version first
	if val, ver, err := sck.IKVClerk.Get(cfgNextKey); err == rpc.ErrNoKey {
		_ = sck.IKVClerk.Put(cfgNextKey, new.String(), 0)
	} else {
		// only update if our new has higher Num to avoid overwriting a newer plan
		old := shardcfg.FromString(val)
		if old.Num < new.Num {
			// best-effort update; ignore errors for 5B if racing
			_ = sck.IKVClerk.Put(cfgNextKey, new.String(), ver)
		}
	}
	// For each shard, if moving from gidA->gidB
	// Keep retrying until Freeze/Install/Delete succeed (OK or ErrVersion),
	// so that we don't publish a config that points clients at a group that
	// doesn't yet own the shard.
	const wait = 5 * time.Millisecond
	for s := 0; s < shardcfg.NShards; s++ {
		sh := shardcfg.Tshid(s)
		from := cur.Shards[s]
		to := new.Shards[s]
		if from == to {
			continue
		}
		dlogf("[sck] move shard %d: %d -> %d", s, from, to)
		if srvs, ok := cur.Groups[from]; ok && from != 0 {
			ckFrom := shardgrp.MakeClerk(sck.clnt, srvs)
			var state []byte
			tries := 0
			t0 := time.Now()
			for {
				st, err := ckFrom.FreezeShard(sh, new.Num)
				tries++
				if err == rpc.OK {
					dlogf("[sck] Freeze shard %d@%d -> OK state=%dB tries=%d dur=%v", s, from, len(st), tries, time.Since(t0))
					state = st
					break
				}
				if err == rpc.ErrVersion {
					dlogf("[sck] Freeze shard %d@%d -> ErrVersion tries=%d dur=%v", s, from, tries, time.Since(t0))
					break
				}
				if tries%50 == 0 {
					dlogf("[sck] Freeze retry shard %d@%d num=%d tries=%d dur=%v", s, from, new.Num, tries, time.Since(t0))
				}
				time.Sleep(wait)
			}
			if dsrvs, ok2 := new.Groups[to]; ok2 && to != 0 {
				ckTo := shardgrp.MakeClerk(sck.clnt, dsrvs)
				tries = 0
				t0 = time.Now()
				for {
					err := ckTo.InstallShard(sh, state, new.Num)
					tries++
					if err == rpc.OK || err == rpc.ErrVersion {
						dlogf("[sck] Install shard %d@%d -> %v tries=%d dur=%v", s, to, err, tries, time.Since(t0))
						break
					}
					if tries%50 == 0 {
						dlogf("[sck] Install retry shard %d@%d num=%d tries=%d dur=%v", s, to, new.Num, tries, time.Since(t0))
					}
					time.Sleep(wait)
				}
			}
			tries = 0
			t0 = time.Now()
			for {
				err := ckFrom.DeleteShard(sh, new.Num)
				tries++
				if err == rpc.OK || err == rpc.ErrVersion {
					dlogf("[sck] Delete shard %d@%d -> %v tries=%d dur=%v", s, from, err, tries, time.Since(t0))
					break
				}
				if tries%50 == 0 {
					dlogf("[sck] Delete retry shard %d@%d num=%d tries=%d dur=%v", s, from, new.Num, tries, time.Since(t0))
				}
				time.Sleep(wait)
			}
		} else {
			dlogf("[sck] skip shard %d from %d (no servers)", s, from)
		}
	}
	const cfgCurKey = "shardcfg/current"
	_ = sck.IKVClerk.Put(cfgCurKey, new.String(), rpc.Tversion(cur.Num))
	dlogf("[sck] published new config num=%d", new.Num)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	const cfgCurKey = "shardcfg/current"
	if sck.IKVClerk == nil {
		log.Fatalf("Query: IKVClerk is nil")
	}
	val, _, err := sck.IKVClerk.Get(cfgCurKey)
	if err == rpc.ErrNoKey {
		// return empty config
		return shardcfg.MakeShardConfig()
	}
	// OK or ErrNoKey already handled
	return shardcfg.FromString(val)
}

