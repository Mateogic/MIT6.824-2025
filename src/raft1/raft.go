package raft

// 实现MIT 6.5840 Lab 3的Raft一致性算法
// 包括领导选举(3A)和日志复制(3B)功能

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// 服务器状态常量
const (
	Follower = iota // 跟随者
	Candidate      // 候选者
	Leader         // 领导者
)

// 时间常量
const (
	HeartbeatInterval       = 120 * time.Millisecond // 心跳间隔
	ElectionTimeoutBaseMs   = 400                    // 选举超时基础时间(毫秒)
	ElectionTimeoutRandomMs = 400                    // 选举超时随机时间(毫秒)
)

// LogEntry 日志条目结构，包含状态机命令和接收时的任期
// 日志条目在论文中是1索引的，但在rf.log切片中是0索引的
// rf.log[0]是哨兵条目
type LogEntry struct {
	Term    int         // 任期
	Command interface{} // 状态机命令
}

// AppendEntries RPC参数结构
type AppendEntriesArgs struct {
	Term         int        // 领导者任期
	LeaderId     int        // 领导者ID，用于跟随者重定向客户端
	PrevLogIndex int        // 新条目前一个日志条目的索引(论文索引)
	PrevLogTerm  int        // prevLogIndex条目的任期
	Entries      []LogEntry // 要存储的日志条目(心跳时为空)
	LeaderCommit int        // 领导者的commitIndex(论文索引)
}

// AppendEntries RPC回复结构
type AppendEntriesReply struct {
	Term    int  // 当前任期，用于领导者更新自己
	Success bool // 如果跟随者包含匹配prevLogIndex和prevLogTerm的条目则为true

	// 用于快速日志冲突解决的优化(3C)
	ConflictTerm  int // 冲突条目的任期
	ConflictIndex int // 该任期第一个条目的论文索引，或跟随者日志长度(如果太短)
}

// Raft对象，实现单个Raft节点
type Raft struct {
	mu        sync.Mutex          // 保护共享状态的锁
	peers     []*labrpc.ClientEnd // 所有节点的RPC端点
	persister *tester.Persister   // 持久化对象
	me        int                 // 本节点在peers数组中的索引
	dead      int32               // 由Kill()设置

	// 所有服务器上的持久状态(响应RPC前需在稳定存储上更新)
	currentTerm int        // 当前任期
	votedFor    int        // 当前任期投票给的候选者ID(无则为-1)
	log         []LogEntry // 日志条目；log[0]是哨兵条目

	// 所有服务器上的易失状态
	state       int // Follower、Candidate或Leader
	commitIndex int // 已知已提交的最高日志条目索引(论文索引)
	lastApplied int // 已应用到状态机的最高日志条目索引(论文索引)

	// 领导者上的易失状态(选举后重新初始化)
	nextIndex  []int // 对每个服务器，下一个要发送的日志条目索引(论文索引)
	matchIndex []int // 对每个服务器，已知已复制的最高日志条目索引(论文索引)

	// 用于向服务/测试器发送已提交消息的通道
	applyCh chan raftapi.ApplyMsg

	// 选举定时器相关
	electionResetEvent time.Time // 最后一次重置选举定时器的事件时间
}

// GetState 返回当前任期和该服务器是否认为自己是领导者
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == Leader)
	return term, isleader
}

// persist 将Raft的持久状态保存到稳定存储
// 见论文Figure 2了解应该持久化什么
func (rf *Raft) persist() {
	// 将持久状态编码为字节数组
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.Save(data, nil)
}

// readPersist 恢复之前持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// 从字节数组解码持久状态
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// 解码失败
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// getLastLogIndex 返回日志中最后一个条目的索引(论文索引)
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// getLastLogTerm 返回日志中最后一个条目的任期
func (rf *Raft) getLastLogTerm() int {
	lastIdx := rf.getLastLogIndex()
	return rf.log[lastIdx].Term
}

// getLogEntryTerm 返回给定论文索引处日志条目的任期
func (rf *Raft) getLogEntryTerm(paperIndex int) int {
	if paperIndex < 0 || paperIndex >= len(rf.log) {
		if paperIndex == 0 && len(rf.log) > 0 {
			return rf.log[0].Term
		}
		return -1
	}
	return rf.log[paperIndex].Term
}

// getLogStartIndex 返回日志中第一个有效索引(哨兵的论文索引)
func (rf *Raft) getLogStartIndex() int {
	return 0
}

// PersistBytes 返回Raft持久化日志的字节数
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot 服务说已创建包含到索引(含)所有信息的快照
// 这意味着服务不再需要通过(含)该索引的日志
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 3D阶段代码
}


// RequestVote RPC参数结构
type RequestVoteArgs struct {
	Term         int // 候选者任期
	CandidateId  int // 请求投票的候选者ID
	LastLogIndex int // 候选者最后日志条目的索引(论文索引)
	LastLogTerm  int // 候选者最后日志条目的任期
}

// RequestVote RPC回复结构
type RequestVoteReply struct {
	Term        int  // 当前任期，用于候选者更新自己
	VoteGranted bool // true表示候选者收到投票
}

// convertToFollower 将Raft节点转换为跟随者状态
// 调用者必须持有rf.mu锁
func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.persist()
}

// RequestVote RPC处理器
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果RPC请求包含更大的任期，更新当前任期并转为跟随者
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 检查候选者日志是否至少和本节点一样新
	myLastLogTerm := rf.getLastLogTerm()
	myLastLogIndex := rf.getLastLogIndex()

	candidateLogIsUpToDate := false
	if args.LastLogTerm > myLastLogTerm {
		candidateLogIsUpToDate = true
	} else if args.LastLogTerm == myLastLogTerm {
		if args.LastLogIndex >= myLastLogIndex {
			candidateLogIsUpToDate = true
		}
	}

	// 如果还没投票或已投给该候选者，且候选者日志足够新，则投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateLogIsUpToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.electionResetEvent = time.Now() // 投票后重置选举定时器
	}
}

// AppendEntries RPC处理器
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果任期小于当前任期则回复false (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果RPC请求包含任期T > currentTerm: 设置currentTerm = T，转换为跟随者
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// 重置选举定时器：对于跟随者来说至关重要，避免在领导者存活时开始选举
	rf.electionResetEvent = time.Now()

	// 如果候选者从新领导者接收到AppendEntries，转换为跟随者
	if rf.state == Candidate && args.Term >= rf.currentTerm {
		rf.convertToFollower(args.Term) // 确保状态为跟随者
	}

	// 如果日志在prevLogIndex处不包含匹配prevLogTerm的条目则回复false (§5.3)
	if args.PrevLogIndex < rf.getLogStartIndex() || args.PrevLogIndex > rf.getLastLogIndex() || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		// 用于快速日志回退的优化 (Lab 3C)
		if args.PrevLogIndex > rf.getLastLogIndex() { // 跟随者日志太短
			reply.ConflictIndex = rf.getLastLogIndex() + 1
			reply.ConflictTerm = -1 // 使用-1表示没有特定任期，只是太短(0是哨兵的有效任期)
		} else { // 现有日志中的冲突
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			conflictSearchIndex := args.PrevLogIndex
			// 找到该任期中的第一个索引(论文索引)
			for conflictSearchIndex > rf.getLogStartIndex() && rf.log[conflictSearchIndex-1].Term == reply.ConflictTerm {
				conflictSearchIndex--
			}
			reply.ConflictIndex = conflictSearchIndex
		}
		return
	}

	// 对于Lab 3A(心跳)，args.Entries为空
	// 日志处理(Figure 2的规则3、4、5)，用于Lab 3B
	
	// 如果现有条目与新条目冲突(索引相同但任期不同)，
	// 删除现有条目及其后续所有条目(§5.3)
	entryIndex := args.PrevLogIndex + 1 // 第一个新条目的论文索引
	logInsertionIndex := 0 // args.Entries[]中的索引
	
	for logInsertionIndex < len(args.Entries) {
		// 如果在此位置有现有条目
		if entryIndex <= rf.getLastLogIndex() {
			// 检查任期是否匹配
			if rf.log[entryIndex].Term != args.Entries[logInsertionIndex].Term {
				// 任期不匹配，删除此条目及其后续所有条目
				rf.log = rf.log[:entryIndex] // 在此位置截断
				rf.persist()
				break
			}
			// 任期匹配，跳过此条目因为它已经正确
		} else {
			// 已到达日志末尾，跳出循环以追加剩余条目
			break
		}
		entryIndex++
		logInsertionIndex++
	}
	
	// 追加任何新条目
	if logInsertionIndex < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[logInsertionIndex:]...)
		rf.persist()
	}
	
	// 如果leaderCommit > commitIndex，设置commitIndex = min(leaderCommit, 最后新条目的索引)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		newCommitIndex := args.LeaderCommit
		lastNewEntryIndex := rf.getLastLogIndex()
		if lastNewEntryIndex < newCommitIndex {
			newCommitIndex = lastNewEntryIndex
		}
		rf.commitIndex = newCommitIndex
		// 如果commitIndex前进，通知applier协程应用新提交的条目
		if rf.commitIndex > oldCommitIndex {
			go rf.applyCommittedEntries()
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

// sendRequestVote 向服务器发送RequestVote RPC的函数
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start 使用Raft的服务(如k/v服务器)想要开始
// 对下一个要追加到Raft日志的命令的协商。如果此
// 服务器不是领导者，返回false。否则开始协商并立即返回。
// 不保证此命令会提交到Raft日志，因为领导者可能失败或输掉选举。
// 即使Raft实例已被杀死，此函数也应优雅返回。
//
// 第一个返回值是命令将出现的索引(如果提交)。
// 第二个返回值是当前任期。
// 第三个返回值表示此服务器是否认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if !isLeader || rf.killed() {
		return index, term, false
	}

	// 创建新日志条目
	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	// 追加到领导者日志
	rf.log = append(rf.log, newEntry)
	rf.persist()
	index = rf.getLastLogIndex() // 新条目的论文索引

	// 立即开始复制到跟随者(异步)
	go func() {
		rf.sendHeartbeats()
	}()

	return index, term, isLeader
}

// Kill 测试器不会停止每个测试后由Raft创建的协程，
// 但它会调用Kill()方法。你的代码可以使用killed()来
// 检查是否已调用Kill()。使用原子操作避免锁的需要。
//
// 问题是长期运行的协程使用内存并可能消耗CPU时间，
// 可能导致后续测试失败并产生混乱的调试输出。
// 任何具有长期运行循环的协程都应调用killed()来检查是否应停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker 是驱动Raft行为的主协程，处理选举超时和心跳
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state

		switch state {
		case Follower, Candidate:
			// 检查选举超时
			timeoutDuration := time.Duration(ElectionTimeoutBaseMs+rand.Intn(ElectionTimeoutRandomMs)) * time.Millisecond
			if time.Since(rf.electionResetEvent) > timeoutDuration {
				rf.becomeCandidateLocked() // 此函数处理RPC的锁定细节
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond) // 频繁轮询超时

		case Leader:
			// 领导者发送心跳。sendHeartbeats内部获取锁
			rf.mu.Unlock() // 调用sendHeartbeats前释放锁
			rf.sendHeartbeats()
			time.Sleep(HeartbeatInterval) // 睡眠心跳间隔
		}
	}
}

// becomeCandidateLocked 转换为候选者状态并开始选举
// 调用者必须在进入时持有rf.mu锁
// 此函数在广播RPC时可能会在内部解锁并重新锁定rf.mu
func (rf *Raft) becomeCandidateLocked() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me // 投票给自己
	rf.persist()
	rf.electionResetEvent = time.Now() // 为此新选举轮次重置选举定时器

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	currentTermWhenStarted := rf.currentTerm // 为协程捕获当前任期

	rf.mu.Unlock() // 广播前解锁，因为sendRequestVote可能阻塞且RPC处理器需要锁
	rf.broadcastRequestVote(currentTermWhenStarted, args)
	rf.mu.Lock()   // 重新获取锁，主ticker循环期望在此调用后持有锁
}

// broadcastRequestVote 向所有其他节点发送RequestVote RPC
// 本身不持有rf.mu，但回调(RPC处理器)会获取它
func (rf *Raft) broadcastRequestVote(electionTerm int, args RequestVoteArgs) {
	var votesReceivedAtomic int32 = 1 // 从1开始表示自投票
	numPeers := len(rf.peers)

	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}

		go func(server int, rpcArgs RequestVoteArgs) { // 按值传递参数
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &rpcArgs, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !ok {
				return
			}

			// 检查是否仍是候选者且在同一选举任期
			if rf.state != Candidate || rf.currentTerm != electionTerm {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				newCount := atomic.AddInt32(&votesReceivedAtomic, 1)
				if newCount > int32(numPeers/2) {
					// 赢得选举
					if rf.state == Candidate && rf.currentTerm == electionTerm { // 双重检查状态和任期
						rf.becomeLeaderLocked()
					}
				}
			}
		}(serverId, args)
	}
}

// becomeLeaderLocked 转换为领导者状态
// 调用者必须持有rf.mu锁
func (rf *Raft) becomeLeaderLocked() {
	if rf.state != Candidate { // 应该只从候选者转换
		return
	}
	rf.state = Leader

	// 为所有跟随者初始化nextIndex和matchIndex (Figure 2)
	lastLogIdx := rf.getLastLogIndex() // 这是论文索引
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIdx + 1 // 论文索引
		rf.matchIndex[i] = 0             // 论文索引，初始化为0
	}

	// 立即发送初始心跳
	rf.mu.Unlock() // sendHeartbeats会重新获取锁
	rf.sendHeartbeats() // 发送初始心跳
	rf.mu.Lock()   // 重新获取锁以满足调用者期望
}

// sendHeartbeats 向所有节点发送AppendEntries RPC(心跳)
// 此函数内部获取和释放rf.mu锁
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex // 用于Lab 3B

	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}

		// prevLogIndex是论文索引。rf.nextIndex存储论文索引
		prevLogIndex := rf.nextIndex[serverId] - 1
		// 确保prevLogIndex有效。如果nextIndex[i]是1(logStartIndex + 1)，则prevLogIndex是0(logStartIndex)
		if prevLogIndex < rf.getLogStartIndex() { // 哨兵索引是getLogStartIndex() (0)
			prevLogIndex = rf.getLogStartIndex() // 如需要，正确指向哨兵
		}
		prevLogTerm := rf.log[prevLogIndex].Term // rf.log是0索引，prevLogIndex是论文索引

		// 对于心跳，Entries为空。对于Lab 3B，这将包含实际日志条目
		entriesToSend := []LogEntry{}

		// Lab 3B: 如果有条目要发送给此跟随者:
		lastLogIndex := rf.getLastLogIndex()
		if rf.nextIndex[serverId] <= lastLogIndex {
			// 确保有效的切片边界
			startIdx := rf.nextIndex[serverId]
			endIdx := lastLogIndex + 1
			if startIdx < len(rf.log) && endIdx <= len(rf.log) && startIdx <= endIdx {
				entriesToSend = rf.log[startIdx:endIdx] // 用论文索引切片rf.log
			}
		}

		args := AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: leaderCommit,
		}

		// 生成协程发送RPC并处理回复，避免阻塞领导者主循环
		go func(server int, rpcArgs AppendEntriesArgs) { // 按值传递参数
			reply := AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", &rpcArgs, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !ok {
				return
			}

			// 检查是否仍是领导者且在同一任期；否则结果过时
			if rf.state != Leader || rf.currentTerm != rpcArgs.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				return
			}

			if reply.Success {
				// 心跳(或日志条目，如果有的话)成功
				// 更新此跟随者的nextIndex和matchIndex
				rf.matchIndex[server] = rpcArgs.PrevLogIndex + len(rpcArgs.Entries) // 论文索引
				rf.nextIndex[server] = rf.matchIndex[server] + 1                   // 论文索引

				// Lab 3B: 如果大多数跟随者已复制到当前任期的某点，更新commitIndex
				rf.updateCommitIndexLocked()
			} else {
				// 跟随者拒绝。递减nextIndex并重试 (Lab 3B/3C)

				// 使用冲突信息更高效地回退nextIndex (Lab 3C)
				if reply.ConflictTerm == -1 { // 跟随者日志太短，ConflictIndex是跟随者日志长度+1
					rf.nextIndex[server] = reply.ConflictIndex // 论文索引
				} else {
					// 在领导者日志中搜索具有ConflictTerm的最后条目(论文索引)
					leaderLastIndexWithConflictTerm := -1
					for i := rf.getLastLogIndex(); i >= rf.getLogStartIndex(); i-- { // 迭代论文索引
						if rf.log[i].Term == reply.ConflictTerm {
							leaderLastIndexWithConflictTerm = i
							break
						}
					}

					if leaderLastIndexWithConflictTerm != -1 {
						// 领导者有ConflictTerm。将nextIndex设置为该任期后的领导者条目
						rf.nextIndex[server] = leaderLastIndexWithConflictTerm + 1 // 论文索引
					} else {
						// 领导者没有ConflictTerm。将nextIndex设置为跟随者的冲突条目
						rf.nextIndex[server] = reply.ConflictIndex // 论文索引
					}
				}
				// 确保nextIndex不低于sentinel_index + 1 (论文索引1)
				if rf.nextIndex[server] <= rf.getLogStartIndex() {
					rf.nextIndex[server] = rf.getLogStartIndex() + 1
				}
			}
		}(serverId, args)
	}
	rf.mu.Unlock() // 迭代节点并生成协程后解锁
}

// updateCommitIndexLocked 基于匹配索引更新提交索引
// 调用者必须持有rf.mu锁
func (rf *Raft) updateCommitIndexLocked() {
	if rf.state != Leader {
		return
	}

	// 找到在大多数服务器上复制的最高索引
	for n := rf.getLastLogIndex(); n > rf.commitIndex; n-- {
		// 只提交当前任期的条目以避免Figure 8问题
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		// 计算有多少服务器有此条目
		count := 1 // 领导者本身
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		// 如果大多数已复制此条目，提交它
		if count > len(rf.peers)/2 {
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = n
			// 如果commitIndex前进，应用新提交的条目
			if rf.commitIndex > oldCommitIndex {
				go rf.applyCommittedEntries()
			}
			break
		}
	}
}

// applyCommittedEntries 由于我们有专门的applier协程，不再需要
// 此函数保留用于兼容性，但什么都不做
func (rf *Raft) applyCommittedEntries() {
	// 专门的applier协程处理应用条目
}

// applier 是将提交的条目应用到状态机的专门协程
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		
		// 检查是否有条目要应用
		if rf.lastApplied < rf.commitIndex {
			// 收集要应用的条目
			startIdx := rf.lastApplied + 1
			endIdx := rf.commitIndex
			
			entriesToApply := make([]raftapi.ApplyMsg, 0, endIdx-startIdx+1)
			
			for i := startIdx; i <= endIdx; i++ {
				entry := rf.log[i]
				msg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: i, // 论文索引
				}
				entriesToApply = append(entriesToApply, msg)
			}
			
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
			
			// 在不持有锁的情况下应用条目
			for _, msg := range entriesToApply {
				rf.applyCh <- msg
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond) // 如果没有要应用的内容则短暂睡眠
		}
	}
}

// Make 服务或测试器想要创建Raft服务器。所有Raft服务器
// (包括此服务器)的端口在peers[]中。此服务器的端口是peers[me]。
// 所有服务器的peers[]数组具有相同的顺序。persister是此服务器
// 保存其持久状态的地方，也初始包含最近保存的状态(如果有)。
// applyCh是测试器或服务期望Raft发送ApplyMsg消息的通道。
// Make()必须快速返回，因此应为任何长期运行的工作启动协程。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh // 存储applyCh

	// 初始化代码 (3A, 3B, 3C)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // 在论文索引0处初始化一个哨兵条目
	rf.log[0] = LogEntry{Term: 0, Command: nil} // 哨兵条目

	rf.commitIndex = 0 // 论文索引
	rf.lastApplied = 0 // 论文索引

	rf.state = Follower
	rf.electionResetEvent = time.Now() // 初始化选举定时器重置事件

	// 初始化领导者上的易失状态(选举后重新初始化，但在此初始化是好的)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 从崩溃前持久化的状态初始化
	rf.readPersist(persister.ReadRaftState())

	// 启动ticker协程以开始选举
	go rf.ticker()

	// 启动applier协程以应用提交的条目
	go rf.applier()

	return rf
}
