# 6.5840 Lab 2: Key/Value Server 实现梳理

## 引言

本项目是 6.5840 分布式系统课程的 Lab 2，目标是构建一个支持 at-most-once Put 和线性一致性的单机键值服务器，并在此基础上实现一个分布式锁。

## KV 服务器实现 (`src/kvsrv1/server.go`)

KV 服务器维护一个内存中的 map 来存储键值对。每个键对应一个结构体，包含值 (`value`) 和版本号 (`version`)。版本号用于实现条件 Put 和 at-most-once 语义。

```go
// ... existing code ...
type KVServer struct {
	mu sync.Mutex

	kvStore map[string]struct {
		value   string
		version rpc.Tversion
	}
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kvStore = make(map[string]struct {
		value   string
		version rpc.Tversion
	})
	return kv
}
// ... existing code ...
```

### Get 方法

`Get(key)` 方法负责获取指定键的值和版本号。如果键存在，返回对应的值、版本和 `rpc.OK`；如果键不存在，返回 `rpc.ErrNoKey`。

```go
// ... existing code ...
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
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
// ... existing code ...
```

### Put 方法

`Put(key, value, version)` 方法负责根据版本号条件地更新或创建键值对。这是实现 at-most-once 的关键。

-   如果键不存在：只有当 `args.Version` 为 0 时才创建新键，初始版本为 1；否则返回 `rpc.ErrNoKey`。
-   如果键存在：只有当 `args.Version` 与服务器当前版本匹配时，才更新值并将服务器版本加 1；否则返回 `rpc.ErrVersion`。

```go
// ... existing code ...
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
// ... existing code ...
```

## KV 客户端实现 (`src/kvsrv1/client.go`)

客户端 (`Clerk`) 负责与服务器进行 RPC 通信，并处理不可靠网络带来的挑战。

### RPC 调用和重试

客户端需要在一个循环中不断重试 RPC 调用，直到收到服务器的回复 (`ck.clnt.Call` 返回 `true`)。在重试前，需要短暂等待 (`time.Sleep`) 以避免过度占用资源。

### 处理 ErrMaybe

在不可靠网络下，客户端重试 Put RPC 时，如果服务器返回 `rpc.ErrVersion`，客户端无法确定是第一次 Put 失败了，还是第一次 Put 成功但回复丢失了，而服务器对重试的 Put 返回了 `ErrVersion`。因此，客户端在重试 Put 收到 `ErrVersion` 时，必须返回 `rpc.ErrMaybe` 给应用层。

```go
// ... existing code ...
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	var reply rpc.GetReply
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {
			return reply.Value, reply.Version, reply.Err
		}
		// Retry on failure or unexpected errors
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	var reply rpc.PutReply
	firstTry := true // Flag to distinguish initial call from retries
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				return rpc.OK
			} else if reply.Err == rpc.ErrNoKey {
				return rpc.ErrNoKey
			} else if reply.Err == rpc.ErrVersion {
				if firstTry {
					return rpc.ErrVersion // First try ErrVersion is definitive failure
				} else {
					return rpc.ErrMaybe // Retry ErrVersion means uncertain outcome
				}
			}
			// any other error from server, retry
		}
		// RPC call failed or server returned an unexpected error, retry
		firstTry = false
		time.Sleep(100 * time.Millisecond)
	}
}
// ... existing code ...
```

## 分布式锁实现 (`src/kvsrv1/lock/lock.go`)

分布式锁是基于 KV 服务器实现的。使用一个特定的键 (`lockKey`) 来表示锁的状态。客户端通过条件 Put 操作来尝试获取和释放锁。

### Acquire 方法

`Acquire` 方法在一个循环中不断尝试获取锁。核心思路是利用 KV 服务器 Put 的版本控制来实现原子性的"测试并设置"操作。

-   获取锁的状态（值和版本）。
-   如果锁键不存在 (`rpc.ErrNoKey`)，尝试用版本 0 Put 自己的 `clientId` 创建键，成功则获取锁。
-   如果锁键存在 (`rpc.OK`)：
    -   如果值是自己的 `clientId`，说明已持有锁，直接返回。
    -   如果值是空字符串 (`""`，约定表示锁是空闲的），尝试用当前版本 Put 自己的 `clientId` 更新键，成功则获取锁。
    -   如果值是其他客户端的 `clientId`，说明锁被他人持有，等待后重试。
-   如果 Put 失败（例如因为版本冲突），等待后重试。

```go
// ... existing code ...
func (lk *Lock) Acquire() {
	for {
		acquired := false

		value, version, errGet := lk.ck.Get(lk.lockKey)
		if errGet == rpc.OK {
			if value == lk.clientId {
				// We already hold the lock.
				acquired = true
			} else if value == "" { // Assuming "" means unlocked
				 // Key exists but lock is free. Try to acquire.
				errPut := lk.ck.Put(lk.lockKey, lk.clientId, version)
				if errPut == rpc.OK {
					acquired = true
				}
			}
			// If value is someone else's ID, loop and retry.
		} else if errGet == rpc.ErrNoKey {
			 // Lock is free (key doesn't exist). Try to acquire by creating it.
			 errPut := lk.ck.Put(lk.lockKey, lk.clientId, 0)
			 if errPut == rpc.OK {
				acquired = true
			 }
		}
		// If Get failed or Put failed (e.g., ErrVersion), loop and retry.

		if acquired {
			return // Successfully acquired the lock
        }

		// If acquisition failed, wait and retry.
		time.Sleep(time.Duration(rand.Intn(100)+10) * time.Millisecond)
	}
}
// ... existing code ...
```

### Release 方法

`Release` 方法也通过循环尝试释放锁。只有当前持有锁的客户端才能成功释放。通过检查键的值是否是自己的 `clientId` 来判断是否持有锁。如果持有，尝试 Put 空字符串 (`""`) 并带上当前版本号来释放锁。由于客户端 Put 可能会遇到 `ErrMaybe`，释放操作在 Put 返回 `rpc.OK` 或 `rpc.ErrMaybe` 时都认为是成功的。

```go
// ... existing code ...
func (lk *Lock) Release() {
	for {
		value, version, errGet := lk.ck.Get(lk.lockKey)

		if errGet == rpc.OK {
			if value == lk.clientId {
				// We hold the lock, try to release it by setting value to ""
				errPut := lk.ck.Put(lk.lockKey, "", version)
				if errPut == rpc.OK || errPut == rpc.ErrMaybe {
					return // Successfully released or release attempt acknowledged
				}
				// If Put failed, retry.
			} else {
				// Lock is held by someone else or is already free.
				return
			}
		} else if errGet == rpc.ErrNoKey {
			// Lock key doesn't exist, implies lock is not held or already released.
			return
		}
		// If Get failed for other reasons, wait and retry.
		time.Sleep(time.Duration(rand.Intn(100)+10) * time.Millisecond)
	}
}
// ... existing code ...
```

## 测试方法

根据实验指导，通过以下命令测试 Lab 2 实现：

-   测试 KV 服务器 (包括可靠和不可靠网络)：
    ```bash
    cd /root/wp/6.5840/src/kvsrv1
    go test -v
    ```
-   测试锁实现 (包括可靠和不可靠网络)：
    ```bash
    cd /root/wp/6.5840/src/kvsrv1/lock
    go test -v
    ```
-   运行竞争检测测试：
    ```bash
    cd /root/wp/6.5840/src/kvsrv1
    go test -race -v
    cd /root/wp/6.5840/src/kvsrv1/lock
    go test -race -v
    ```

如果所有测试都通过，并且竞争检测没有报告问题，那么你的 Lab 2 实现就是正确的。

这份文档概括了 Lab 2 的核心实现思路和关键代码片段。希望对你理解和回顾代码有所帮助！ 