package rsm

import (
	"sync/atomic"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"

)

var useRaftStateMachine bool // to plug in another raft besided raft1


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  uint64
	Req any
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	nextId   uint64
	waitCh   map[int]chan applyResult // index -> waiter
	doneCh   chan struct{}
	closed   bool
	lastApplied int
}

type applyResult struct {
	id  uint64
	rep any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waitCh:       make(map[int]chan applyResult),
		doneCh:       make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	// Restore snapshot on restart if present
	if snap := persister.ReadSnapshot(); len(snap) > 0 {
		sm.Restore(snap)
	}
	// start reader goroutine
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	id := atomic.AddUint64(&rsm.nextId, 1)
	op := Op{Id: id, Req: req}

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan applyResult, 1)
	rsm.mu.Lock()
	rsm.waitCh[index] = ch
	rsm.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	// overall safety timeout to avoid indefinite blocking if no progress
	overall := time.NewTimer(2 * time.Second)
	defer overall.Stop()
	for {
		select {
		case res, ok := <-ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			if res.id == id {
				return rpc.OK, res.rep
			}
			return rpc.ErrWrongLeader, nil
		case <-ticker.C:
			curTerm, isLead := rsm.rf.GetState()
			if !isLead || curTerm != term {
				// leadership changed or lost; stop waiting
				rsm.mu.Lock()
				delete(rsm.waitCh, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		case <-overall.C:
			// give up after a while to avoid test hangs
			rsm.mu.Lock()
			delete(rsm.waitCh, index)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		case <-rsm.doneCh:
			return rpc.ErrWrongLeader, nil
		}
	}
}

// Reader goroutine: consumes applyCh, applies ops to the service state machine,
// coordinates Submit waiters, and triggers snapshots when needed.
func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			// apply operation to state machine
			rsm.lastApplied = msg.CommandIndex
			op, ok := msg.Command.(Op)
			if !ok {
				// unknown command type; just ignore but wake waiter if any
				rsm.mu.Lock()
				if ch, ok := rsm.waitCh[msg.CommandIndex]; ok {
					ch <- applyResult{0, nil}
					close(ch)
					delete(rsm.waitCh, msg.CommandIndex)
				}
				rsm.mu.Unlock()
				continue
			}
			rep := rsm.sm.DoOp(op.Req)

			// notify waiter if this server is the one that submitted
			rsm.mu.Lock()
			if ch, ok := rsm.waitCh[msg.CommandIndex]; ok {
				ch <- applyResult{op.Id, rep}
				close(ch)
				delete(rsm.waitCh, msg.CommandIndex)
			}
			// maybe snapshot after applying
			if rsm.maxraftstate >= 0 && rsm.rf.PersistBytes() > rsm.maxraftstate {
				snapshot := rsm.sm.Snapshot()
				// snapshot up to and including last applied index
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
			}
			rsm.mu.Unlock()
		} else if msg.SnapshotValid {
			// restore snapshot
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Lock()
			if msg.SnapshotIndex > rsm.lastApplied {
				rsm.lastApplied = msg.SnapshotIndex
			}
			rsm.mu.Unlock()
		}
	}
	// applyCh closed by Raft -> signal shutdown and unblock waiters
	rsm.mu.Lock()
	if !rsm.closed {
		close(rsm.doneCh)
		rsm.closed = true
	}
	for idx, ch := range rsm.waitCh {
		close(ch)
		delete(rsm.waitCh, idx)
	}
	rsm.mu.Unlock()
}
