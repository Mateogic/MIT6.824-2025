package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvStore map[string]struct {
		value   string
		version rpc.Tversion
	}
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvStore = make(map[string]struct {
		value   string
		version rpc.Tversion
	})
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data, ok := kv.kvStore[args.Key]; ok {
		reply.Value = data.value
		reply.Version = data.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data, exists := kv.kvStore[args.Key]

	if !exists {
		if args.Version == 0 {
			// Create new key
			kv.kvStore[args.Key] = struct {
				value   string
				version rpc.Tversion
			}{args.Value, 1}
			reply.Err = rpc.OK
		} else {
			// Key doesn't exist, and Put version is > 0
			reply.Err = rpc.ErrNoKey
		}
	} else {
		// Key exists
		if args.Version == data.version {
			// Versions match, update value and increment version
			kv.kvStore[args.Key] = struct {
				value   string
				version rpc.Tversion
			}{args.Value, data.version + 1}
			reply.Err = rpc.OK
		} else {
			// Versions don't match
			reply.Err = rpc.ErrVersion
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
