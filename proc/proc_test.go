package proc

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitOrFail(t *testing.T, ch <-chan struct{}, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(d):
		t.Fatalf("timeout waiting: %v", msg)
	}
}

func TestZeroValue_InitAndDone(t *testing.T) {
	var m Manager

	d1 := m.Done()
	if d1 == nil {
		t.Fatalf("Done() returned nil channel")
	}
	d2 := m.Done()
	if d1 != d2 {
		t.Fatalf("Done() must return the same channel instance")
	}

	m.Stop()
	select {
	case <-m.Done():
	default:
		t.Fatalf("Done() not closed after Stop()")
	}
}

func TestZeroValue_ContextCanceledOnStop(t *testing.T) {
	var m Manager

	ctx := m.Context(context.Background()) // should be safe on zero value
	select {
	case <-ctx.Done():
		t.Fatalf("context should not be canceled before Stop()")
	default:
	}

	m.Stop()
	waitOrFail(t, ctx.Done(), 200*time.Millisecond, "ctx.Done after Stop")
}

func TestWorker_DisabledAfterStop(t *testing.T) {
	var m Manager
	m.Stop()

	w := m.Add()
	if !w.Disabled() {
		t.Fatalf("expected disabled worker after Stop()")
	}

	select {
	case <-w.Done():
	default:
		t.Fatalf("disabled worker Done() must be closed")
	}

	ctx := w.Context(context.Background())
	select {
	case <-ctx.Done():
	default:
		t.Fatalf("disabled worker Context() must be canceled")
	}

	w.Complete() // should not panic
}

func TestWait_BlocksUntilStop(t *testing.T) {
	var m Manager

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Wait()
	}()

	select {
	case <-done:
		t.Fatalf("Wait returned before Stop()")
	case <-time.After(200 * time.Millisecond):
	}

	m.Stop()
	waitOrFail(t, done, 500*time.Millisecond, "Wait after Stop")
}

func TestWait_WaitsForWorkers(t *testing.T) {
	var m Manager

	release := make(chan struct{})
	w := m.Add()
	if w.Disabled() {
		t.Fatalf("worker should not be disabled")
	}

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		m.Stop()
		m.Wait()
	}()

	select {
	case <-waitDone:
		t.Fatalf("Wait returned while worker not finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	w.Complete()

	waitOrFail(t, waitDone, 500*time.Millisecond, "Wait waiting for worker completion")
	if m.Count() != 0 {
		t.Fatalf("expected Count() == 0, got %d", m.Count())
	}
}

func TestGo_Helper(t *testing.T) {
	var m Manager

	started := make(chan struct{})
	finished := make(chan struct{})

	m.Go(func() {
		close(started)
		time.Sleep(100 * time.Millisecond)
		close(finished)
	})

	waitOrFail(t, started, 200*time.Millisecond, "Go started")
	if m.Count() <= 0 {
		// Count is eventually consistent; but it should be positive here
		t.Fatalf("expected Count() > 0 while goroutine running, got %d", m.Count())
	}

	m.Stop()
	m.Wait()
	select {
	case <-finished:
	default:
		t.Fatalf("expected function to finish before Wait returns")
	}
	if m.Count() != 0 {
		t.Fatalf("expected Count() == 0 after Wait, got %d", m.Count())
	}
}

func TestWaitContext_StopPhaseTimeout(t *testing.T) {
	var m Manager

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := m.WaitContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestWaitContext_WorkPhaseTimeout(t *testing.T) {
	var m Manager

	w := m.Add()
	if w.Disabled() {
		t.Fatalf("worker should not be disabled")
	}
	m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := m.WaitContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestWaitTimeout_StopPhase(t *testing.T) {
	var m Manager
	err := m.WaitTimeout(50 * time.Millisecond)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestWaitTimeout_WorkPhase(t *testing.T) {
	var m Manager
	w := m.Add()
	m.Stop()

	err := m.WaitTimeout(50 * time.Millisecond)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}

	w.Complete()
	m.Wait()
}

func TestStop_Idempotent(t *testing.T) {
	var m Manager
	m.Stop()
	m.Stop()
	m.Stop()

	select {
	case <-m.Done():
	default:
		t.Fatalf("Done must be closed after Stop")
	}
}

func TestFinally_Once(t *testing.T) {
	var m Manager
	var calls atomic.Int64

	err1 := m.Once(func() error {
		calls.Add(1)
		return errors.New("x")
	})
	err2 := m.Once(func() error {
		calls.Add(1)
		return nil
	})

	if calls.Load() != 1 {
		t.Fatalf("expected Once called once, got %d", calls.Load())
	}
	if err1 == nil || err1.Error() != "x" {
		t.Fatalf("expected first error 'x', got %v", err1)
	}
	if err2 != nil {
		t.Fatalf("expected subsequent Once to return nil error (no-op), got %v", err2)
	}
}

func TestCloseTimeout_ErrorPrecedence(t *testing.T) {
	var m Manager
	w := m.Add()

	err := m.CloseTimeout(50*time.Millisecond, func() error {
		return errors.New("final")
	})

	if err == nil || err.Error() != "final" {
		t.Fatalf("expected finalizer error, got %v", err)
	}

	w.Complete()
	_ = m.Close(nil)
}

func TestAdmissionGate_NoWaitGroupRaceUnderConcurrency(t *testing.T) {
	var m Manager
	var wg sync.WaitGroup

	workers := runtime.NumCPU()
	runs := 2000

	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < runs; j++ {
				w := m.Add()
				if !w.Disabled() {
					runtime.Gosched()
					w.Complete()
				}
			}
		}()
	}

	close(start)

	go func() {
		time.Sleep(5 * time.Millisecond)
		m.Stop()
	}()

	m.Wait()

	wg.Wait()

	if m.Count() != 0 {
		t.Fatalf("expected Count() == 0 after Wait, got %d", m.Count())
	}
}

var cnt = uint64(1)

func BenchmarkWorker(b *testing.B) {

	var p Manager
	wrk := p.Add()
	defer wrk.Complete()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		func() {
			w := p.Add()
			defer w.Complete()
			cnt++
		}()
	}

	b.Log(cnt)
}
