# MIT 6.5840 Lab 1: MapReduce

## 解题思路与实现方式

本实验要求实现一个简化的分布式 MapReduce 系统，包括一个协调器（Coordinator）和多个工作进程（Worker）。协调器负责任务分配和故障处理，工作进程负责执行 Map 和 Reduce 任务。

### 1. 系统设计概述

系统采用经典的协调器-工作进程模型。工作进程通过 Go 的 RPC 机制与协调器通信，请求任务、报告任务完成。协调器维护所有任务的状态，并在工作进程发生故障（通过超时判断）时重新分配任务。

### 2. 实现细节

主要修改了 `mr/rpc.go`, `mr/coordinator.go`, `mr/worker.go` 这三个文件。

#### `mr/rpc.go` - RPC 消息定义

定义了协调器和工作进程之间通信所需的 RPC 参数和回复结构体：

- `TaskRequest`: Worker 用于向协调器请求任务，包含 Worker 的 ID。
```go
type TaskRequest struct {
	WorkerId int
}
```
- `TaskResponse`: 协调器返回给 Worker 的任务信息，包括任务类型（Map, Reduce, Exit, Wait）、任务 ID、输入文件（Map 任务）、中间文件列表（Reduce 任务）以及 Reduce 任务总数。
```go
type TaskResponse struct {
	TaskType int // 0 for Map, 1 for Reduce, 2 for Exit, 3 for Wait
	TaskId int
	InputFile string // For Map tasks
	ReduceFiles []string // For Reduce tasks
	NReduce int // Total number of reduce tasks
}
```
- `TaskCompleteRequest`: Worker 用于向协调器报告任务完成，包含任务类型、任务 ID 和任务是否成功。
```go
type TaskCompleteRequest struct {
	TaskType int
	TaskId int
	Success bool
}
```
- `TaskCompleteResponse`: 协调器接收任务完成报告后的回复（目前为空）。
```go
type TaskCompleteResponse struct {
	// Nothing needed for now
}
```

#### `mr/coordinator.go` - 协调器实现

协调器负责初始化任务、分配任务、跟踪任务状态和处理故障。

- **`Coordinator` 结构体**: 增加了字段来存储任务信息和状态。
```go
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	Id int
	Type int // 0 for Map, 1 for Reduce
	InputFile string // For Map tasks
	ReduceFiles []string // For Reduce tasks
	State TaskState
	Start_Time time.Time // 用于故障检测
}

type Coordinator struct {
	Files []string // 输入文件列表
	NReduce int // Reduce 任务数量
	MapTasks []Task // Map 任务列表
	ReduceTasks []Task // Reduce 任务列表
	Mutex sync.Mutex // 保护共享数据
	MapTaskCount int // Map 任务总数
	ReduceTaskCount int // Reduce 任务总数
	MapCompletedCount int // 已完成 Map 任务计数
	ReduceCompletedCount int // 已完成 Reduce 任务计数

	IntermediateFiles [][]string // IntermediateFiles[reduceTaskId] 存储属于该 Reduce 任务的中间文件列表
}
```
- **`MakeCoordinator` 函数**: 初始化 `Coordinator` 结构体，根据输入文件和 `nReduce` 参数创建初始的 Map 任务和 Reduce 任务列表，并将它们的状态设置为 `Idle`。
```go
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// ... 初始化 Coordinator 结构体字段 ...

	// Initialize Map tasks
	for i, file := range files {
		c.MapTasks[i] = Task{
			Id: i,
			Type: MapTask,
			InputFile: file,
			State: Idle,
			Start_Time: time.Time{},
		}
	}

	// Initialize Reduce tasks
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			Id: i,
			Type: ReduceTask,
			State: Idle,
			Start_Time: time.Time{},
		}
	}

	// 初始化 IntermediateFiles
	c.IntermediateFiles = make([][]string, nReduce)
	for i := 0; i < nReduce; i++ {
		c.IntermediateFiles[i] = []string{}
	}

	// ... 启动 RPC 服务器 ...
	return &c
}
```
- **`RequestTask` RPC 处理函数**: Worker 调用此函数请求任务。函数会检查是否有 `Idle` 或已超时的 `InProgress` 状态的 Map 任务可分配。如果所有 Map 任务都已完成，则检查是否有可分配的 Reduce 任务。如果没有可用任务但作业未完成，指示 Worker 等待 (`WaitTask`)；如果所有任务都已完成，指示 Worker 退出 (`ExitTask`)。
```go
func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// ... 分配 Map 任务或 Reduce 任务 ...

	// 如果所有任务都已完成，返回 ExitTask
	if c.ReduceCompletedCount == c.NReduce {
		reply.TaskType = ExitTask
		// ... log ...
		return nil
	}

	// 如果没有可用任务，返回 WaitTask
	reply.TaskType = WaitTask
	// ... log ...
	return nil
}
```
- **`TaskComplete` RPC 处理函数**: Worker 调用此函数报告任务完成。协调器根据报告更新任务状态和完成计数。Map 任务完成后，将生成的中间文件名添加到 `IntermediateFiles` 列表中。
```go
func (c *Coordinator) TaskComplete(args *TaskCompleteRequest, reply *TaskCompleteResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if args.TaskType == MapTask {
		// ... 更新 Map 任务状态和计数 ...
		// 收集中间文件信息
		for i := 0; i < c.NReduce; i++ {
			c.IntermediateFiles[i] = append(c.IntermediateFiles[i], fmt.Sprintf("mr-%d-%d", args.TaskId, i))
		}
	} else if args.TaskType == ReduceTask {
		// ... 更新 Reduce 任务状态和计数 ...
	}

	return nil
}
```
- **`Done` 函数**: 检查是否所有 Map 和 Reduce 任务都已完成，返回布尔值指示作业是否结束。
```go
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	return c.MapCompletedCount == len(c.Files) && c.ReduceCompletedCount == c.NReduce
}
```

#### `mr/worker.go` - 工作进程实现

工作进程负责向协调器请求任务、执行任务并报告结果。

- **`Worker` 函数**: 工作进程的主循环。不断调用 `call` 函数请求任务，并根据返回的任务类型执行相应逻辑。
```go
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := os.Getpid()

	for {
		args := TaskRequest{WorkerId: workerId}
		reply := TaskResponse{}
		ok := call("Coordinator.RequestTask", &args, &reply)

		if !ok {
			// Coordinator is likely done or crashed, exit
			break
		}

		switch reply.TaskType {
		case MapTask:
			// 执行 Map 任务逻辑 ...
			// ... 调用 mapf ...
			// ... 将中间结果写入文件 (mr-mapId-reduceId) ...
			// ... 报告任务完成 ...

		case ReduceTask:
			// 执行 Reduce 任务逻辑 ...
			// ... 读取中间文件 (mr-*-reduceId) ...
			// ... 排序 ...
			// ... 调用 reducef ...
			// ... 将最终结果写入文件 (mr-out-reduceId) ...
			// ... 报告任务完成 ...

		case ExitTask:
			return // 退出循环

		case WaitTask:
			time.Sleep(time.Second) // 等待后重试
		}
	}
}
```
- **Map 任务执行**: 读取输入文件，调用应用程序的 `mapf` 函数，根据 `ihash` 将生成的 key/value 对分配到不同的 Reduce 桶，并将每个桶的内容写入一个临时的中间文件（命名为 `mr-mapId-reduceId`），然后原子地重命名为最终文件名。完成后向协调器报告。
- **Reduce 任务执行**: 读取所有与当前 Reduce 任务相关的中间文件（命名模式为 `mr-*-reduceId`），解析 JSON 数据，按 key 排序。然后遍历排序后的 key/value 对，对每个唯一的 key 调用应用程序的 `reducef` 函数。将 `reducef` 的输出格式化（`%v %v`）写入一个临时的最终输出文件（命名为 `mr-out-reduceId`），然后原子地重命名。完成后向协调器报告。
- **`call` 辅助函数**: 用于向协调器发送 RPC 请求。修改后，如果在拨号或调用 RPC 时失败，返回 `false`，以便 Worker 能够检测到协调器可能已退出并相应地退出。

### 3. 关键机制

- **任务状态**: 使用 `Idle`, `InProgress`, `Completed` 三种状态跟踪任务进展。
- **故障处理**: 协调器通过检查 `InProgress` 任务的开始时间来检测故障。如果一个任务执行超过 10 秒，协调器会认为执行该任务的 Worker 可能已崩溃，并将该任务重新标记为 `Idle`，以便可以重新分配给其他 Worker。
- **中间文件处理**: Map 任务将中间结果分散到 `nReduce` 个文件中，每个文件对应一个 Reduce 任务。文件命名约定为 `mr-mapId-reduceId`。为了处理 Worker 崩溃，写入文件时先使用临时文件，写完后再原子地重命名。
- **工作进程退出**: 当所有任务完成后，协调器会在 Worker 请求任务时返回 `ExitTask`。Worker 收到 `ExitTask` 或无法连接到协调器时会退出。

### 4. 如何运行和测试

1.  进入 `src/main` 目录。
2.  构建应用程序插件（例如，词频统计）：`go build -buildmode=plugin ../mrapps/wc.go`。
3.  在第一个终端运行协调器：`go run mrcoordinator.go pg-*.txt`。
4.  在一个或多个其他终端运行工作进程：`go run mrworker.go wc.so`。
5.  使用测试脚本进行自动化测试：`bash test-mr.sh`。可以使用 `go run -race` 运行协调器和工作进程来检测竞态条件。

通过上述实现，你的 MapReduce 系统应该能够正确地执行 MapReduce 作业，并在 Worker 发生故障时进行恢复，最终通过 Lab 1 的所有测试。 