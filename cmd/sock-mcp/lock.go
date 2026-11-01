package main

import (
	"context"
	"os"
)

// lockFile blocks until f holds its exclusive lock, or ctx ends. The returned
// func releases the lock and closes f. The operating system frees a dead
// holder's lock.
func lockFile(ctx context.Context, f *os.File) (func(), error) {
	done := make(chan error, 1)
	go func() { done <- lockExclusive(f) }()
	select {
	case err := <-done:
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return func() { _ = f.Close() }, nil
	case <-ctx.Done():
		_ = f.Close() // frees a blocked flock; a blocked LockFileEx waits out the holder
		return nil, ctx.Err()
	}
}
