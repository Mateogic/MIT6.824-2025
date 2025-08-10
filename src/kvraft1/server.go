package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"

)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu      sync.Mutex
	kv      map[string]string
	vers    map[string]rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch v := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if val, ok := kv.kv[v.Key]; ok {
			return rpc.GetReply{Value: val, Version: kv.vers[v.Key], Err: rpc.OK}
		}
		return rpc.GetReply{Err: rpc.ErrNoKey}
	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		curVal, exists := kv.kv[v.Key]
		curVer := kv.vers[v.Key]
		if !exists {
			if v.Version == 0 {
				kv.kv[v.Key] = v.Value
				kv.vers[v.Key] = 1
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		// exists
		if v.Version == curVer {
			kv.kv[v.Key] = v.Value
			kv.vers[v.Key] = curVer + 1
			return rpc.PutReply{Err: rpc.OK}
		}
		_ = curVal // unused except for clarity
		return rpc.PutReply{Err: rpc.ErrVersion}
	default:
		// unknown op
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// encode as two maps to avoid unexported fields
	e.Encode(kv.kv)
	e.Encode(kv.vers)
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
	if d.Decode(&m1) == nil && d.Decode(&m2) == nil {
		kv.mu.Lock()
		kv.kv = m1
		kv.vers = m2
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
	r := rep.(rpc.GetReply)
	*reply = r
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
	r := rep.(rpc.PutReply)
	*reply = r
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, kv: make(map[string]string), vers: make(map[string]rpc.Tversion)}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
