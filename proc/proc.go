// Package proc provides a small work/goroutine coordination helper for graceful shutdown.
package proc

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var ErrTimeout = errors.New("timed out")

// Manager coordinates graceful shutdown of workers and goroutines.
// The zero value is ready to use. A Manager must not be copied after first use.
type Manager struct {
	stop   chan struct{}
	done   sync.WaitGroup
	stopMu sync.RWMutex

	initOnce sync.Once
	stopOnce sync.Once
	callOnce sync.Once

	count atomic.Int64
}

func (m *Manager) init() {
	m.initOnce.Do(func() {
		m.stop = make(chan struct{})
	})
}

// Add registers a unit of work with the Manager and returns a Worker handle.
// Users of the returned Worker must call Complete when their work is finished.
//
// Add is safe to call even if the Manager is already stopped and its Wait
// method has already been called.
//
// If the Manager is already stopped, the returned Worker is disabled:
// its methods will return closed channels and canceled contexts.
func (m *Manager) Add() Worker {
	m.init()
	m.stopMu.RLock()
	select {
	case <-m.stop:
		m.stopMu.RUnlock()
		return Worker{}
	default:
		m.done.Add(1)
		m.stopMu.RUnlock()
		m.count.Add(1)
		return Worker{m}
	}
}

// Go calls fn in a new goroutine and increases the number of active workers.
// When fn returns, the number of active workers is decreased.
// If the Manager is already stopped, fn is not called.
func (m *Manager) Go(fn func()) {
	w := m.Add()
	if w.Disabled() {
		return
	}
	go func(w Worker) {
		defer w.Complete()
		fn()
	}(w)
}

// Done returns a channel that is closed when the Manager is stopped.
func (m *Manager) Done() <-chan struct{} {
	m.init()
	return m.stop
}

// Count returns the number of active workers.
func (m *Manager) Count() int {
	return int(m.count.Load())
}

// Context returns a context that is canceled when the Manager is stopped,
// or when the parent context is canceled or expires.
func (m *Manager) Context(parent context.Context) context.Context {
	m.init()
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	select {
	case <-m.stop:
		cancel()
		return ctx
	default:
	}
	go func() {
		defer cancel()
		select {
		case <-m.stop:
		case <-parent.Done():
		}
	}()
	return ctx
}

// Stop signals all workers to stop.
func (m *Manager) Stop() {
	m.init()
	m.stopOnce.Do(func() {
		m.stopMu.Lock()
		close(m.stop)
		m.stopMu.Unlock()
	})
}

// Wait waits until the Manager is stopped and then until all workers report completion.
//
// Wait is not analogous to sync.WaitGroup's Wait,
// it will not return even if all the workers report completion
// but the Manager is still in active state (not stopped).
func (m *Manager) Wait() {
	m.init()
	<-m.stop
	m.done.Wait()
}

// WaitContext waits until Manager is stopped and then until all workers
// report completion. If the provided context is canceled or expires
// during any of these steps, WaitContext returns ctx.Err().
//
// WaitContext is not analogous to sync.WaitGroup's Wait,
// it will not return even if all the workers report completion
// but the Manager is still in active state (not stopped).
//
// WaitContext may leak a goroutine if ctx is done and the Manager is never stopped.
func (m *Manager) WaitContext(ctx context.Context) error {
	m.init()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.stop:
	}

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		m.Wait()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// WaitTimeout waits until Manager is stopped and then until all workers
// report completion. If the provided timeout expires during any of these steps,
// WaitTimeout returns ErrTimeout.
//
// WaitTimeout is not analogous to sync.WaitGroup's Wait,
// it will not return even if all the workers report completion
// but the Manager is still in active state (not stopped).
//
// WaitTimeout may leak a goroutine if the timeout expires
// and the Manager is never stopped.
func (m *Manager) WaitTimeout(duration time.Duration) error {
	m.init()

	t := time.NewTimer(duration)

	select {
	case <-t.C:
		return ErrTimeout
	case <-m.stop:
	}

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		m.Wait()
	}()

	select {
	case <-t.C:
		return ErrTimeout
	case <-ch:
		return nil
	}
}

// Once calls the provided fn exactly once.
// Subsequent calls to Once are no-ops.
// If fn is nil, Once ignores it but still counts as performed.
func (m *Manager) Once(fn func() error) (err error) {
	m.callOnce.Do(func() {
		if fn != nil {
			err = fn()
		}
	})
	return
}

// Close is a shorthand for Stop, Wait, and Once.
// See mentioned methods for details.
//
// The provided fn can be nil.
func (m *Manager) Close(fn func() error) error {
	m.Stop()
	m.Wait()
	return m.Once(fn)
}

// CloseTimeout is a shorthand for Stop, WaitTimeout and Once.
// See mentioned methods for details.
//
// Once is called regardless of the error returned by WaitTimeout.
// The error returned by CloseTimeout is either from Once or
// from WaitTimeout in that particular order.
//
// The provided fn can be nil.
func (m *Manager) CloseTimeout(duration time.Duration, fn func() error) error {
	m.Stop()
	werr := m.WaitTimeout(duration)
	ferr := m.Once(fn)
	if ferr != nil {
		return ferr
	}
	return werr
}

// CloseContext is a shorthand for Stop, WaitContext and Once.
// See mentioned methods for details.
//
// Once is called regardless of the error returned by WaitContext.
// The error returned by CloseContext is either from Once or
// from WaitContext in that particular order.
//
// The provided fn can be nil.
func (m *Manager) CloseContext(ctx context.Context, fn func() error) error {
	m.Stop()
	werr := m.WaitContext(ctx)
	ferr := m.Once(fn)
	if ferr != nil {
		return ferr
	}
	return werr
}

// Watch stops the Manager when the provided channel is closed
// or a value is received from it.
func (m *Manager) Watch(ch <-chan struct{}) {
	m.init()
	go func() {
		select {
		case <-ch:
			m.Stop()
		// case _, ok := <-ch:
		// 	if !ok {
		// 		m.Stop()
		// 	}
		case <-m.stop:
		}
	}()
}

// WatchContext stops the Manager when the provided ctx is done.
func (m *Manager) WatchContext(ctx context.Context) {
	m.init()
	go func() {
		select {
		case <-ctx.Done():
			m.Stop()
		case <-m.stop:
		}
	}()
}

/**/

// Worker represents a registered unit of work tied to a Manager.
type Worker struct{ mgr *Manager }

// Done returns a channel that is closed when the worker should stop its work.
// If the Worker was created from an already stopped Manager,
// Done returns a closed channel.
func (w Worker) Done() <-chan struct{} {
	if w.mgr != nil {
		return w.mgr.Done()
	}
	return closedCh
}

// Context returns a context that is canceled when the worker is stopped
// or the provided parent is canceled or expires.
//
// If the Worker was created from an already stopped Manager,
// Context returns a canceled context.
func (w Worker) Context(parent context.Context) context.Context {
	if w.mgr != nil {
		return w.mgr.Context(parent)
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	cancel()
	return ctx
}

// Complete reports that the unit of work is finished.
// It must be called exactly once.
// If the Worker was created from a stopped Manager, Complete is a no-op.
func (w Worker) Complete() {
	if w.mgr != nil {
		w.mgr.done.Done()
		w.mgr.count.Add(-1)
	}
}

// Stopped reports whether the Worker is stopped.
func (w Worker) Stopped() bool {
	if w.mgr == nil {
		return true
	}
	select {
	case <-w.mgr.Done():
		return true
	default:
		return false
	}
}

// Disabled reports whether the Worker was created from a stopped Manager.
// It only checks a single pointer and is a bit faster than Stopped.
func (w Worker) Disabled() bool {
	return w.mgr == nil
}

// Add registers another unit of work in the same Manager.
func (w Worker) Add() Worker {
	if w.mgr != nil {
		return w.mgr.Add()
	}
	return Worker{}
}

var closedCh = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

/**/

var (
	term     chan struct{}
	termCtx  context.Context
	termStop func()
	rootOnce sync.Once
)

func initSignals() {
	rootOnce.Do(func() {
		termCtx, termStop = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
		term = make(chan struct{})
		go func() {
			<-termCtx.Done()
			close(term)
			termStop()
		}()
	})
}

// Term returns a channel that is closed when one of the stop signals is received:
// os.Interrupt, syscall.SIGTERM, syscall.SIGHUP.
//
// Signal notifiers are not initialized until Term or TermContext are called.
func Term() <-chan struct{} {
	initSignals()
	return term
}

// TermContext returns a context that is canceled when one of the stop signals
// is received: os.Interrupt, syscall.SIGTERM, syscall.SIGHUP.
//
// Signal notifiers are not initialized until Term or TermContext are called.
func TermContext() context.Context {
	initSignals()
	return termCtx
}
