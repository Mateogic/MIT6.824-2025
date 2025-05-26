package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"time"

	crand "crypto/rand"
	"encoding/base64"
	"math/rand"
)

// Generate a random string of length n
func RandValue(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	return base64.URLEncoding.EncodeToString(b)[:n]
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey  string
	clientId string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockKey = l
	lk.clientId = RandValue(8) // Generate a unique client ID
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// Try to acquire the lock by putting our clientId as the value.
		// If the key doesn't exist, Put with version 0.
		// If the key exists, try to Put with the current version.

		acquired := false

		value, version, errGet := lk.ck.Get(lk.lockKey)
		if errGet == rpc.OK {
			if value == lk.clientId {
				// We already hold the lock. This can happen if Release failed or ErrMaybe happened.
				acquired = true
			} else if value == "" { // Assuming "" means unlocked
				 // Key exists but lock is free. Try to acquire by updating with current version.
				errPut := lk.ck.Put(lk.lockKey, lk.clientId, version)
				if errPut == rpc.OK {
					acquired = true
				}
			}
			// If value is someone else's ID, loop and retry.
		} else if errGet == rpc.ErrNoKey {
			 // Lock is free (key doesn't exist). Try to acquire by creating it.
			 errPut := lk.ck.Put(lk.lockKey, lk.clientId, 0) // Version 0 to create
			 if errPut == rpc.OK {
				acquired = true
			 }
		}
		// If Get failed or Put failed with ErrVersion (someone else got it) or other error, loop and retry.

		if acquired {
			return // Successfully acquired the lock
        }

		// If acquisition failed, wait and retry.
		time.Sleep(time.Duration(rand.Intn(100)+10) * time.Millisecond)
	}
}

// Refined Release logic based on "" meaning unlocked.
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
				// If Put failed (e.g. network, or server error other than version), retry.
			} else {
				// Lock is held by someone else or is already free (value != myId).
                // We should not attempt to release it.
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
