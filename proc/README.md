## proc

Goroutine coordinator and shutdown manager.

`proc.Manager` is **not** a general-purpose replacement for `sync.WaitGroup`.
It is designed to simplify shutdown management and protect running code 
from being interrupted.

### Key properties

- Worker registration (`Add`) is gated by an `RWMutex` shared with `Stop`,
  preventing `Add` from racing with `Wait`.

- `Wait` is **not** analogous to `sync.WaitGroup`'s `Wait`:
  it will **not** return even if all workers have reported completion 
  but the `Manager` is still active (not stopped).

- `Add` may be called at any time; if the manager is already stopped,
  the returned `Worker` is disabled and acts as a no-op.

- `Add` can be used in synchronous code as well.

A `Manager` has two phases: active and stopped.\
Workers may be added while the manager is active.\
Calling `Stop` transitions the manager into the stopped state and signals
all registered workers to stop.

### Use cases

```go
type MyService struct {
    // ...
    proc proc.Manager
}

func New(...) *MyService {
    s := new(MyService)
    
    // ...
    
    go s.doSomething(s.proc.Add())
    // or
    s.proc.Go(s.doSomething)
    
    return s
}

func (s *MyService) doSomething(w proc.Worker) {
    defer w.Complete()
    
    // ...
    
    select {
    case <-w.Done(): // like context
        return
    default:
    }
    
    // ...
}

func (s *MyService) Close() error {
    return s.proc.Close(func() error {
        // close resources
    })
    
    // Close is a shorthand for
    // s.proc.Stop()
    // s.proc.Wait()
    // return s.proc.Once(func() error { ... })
    
    // there are also WaitContext/CloseContext and 
    // WaitTimeout/CloseTimeout variants
}

func (s *MyService) NormalMethodThatMustFinish() error {
    w := s.proc.Add()
    if w.Disabled() {
        return errors.New("service closed")
    }
    defer w.Complete()
    
    // ...
}
```

See [GoDoc](https://pkg.go.dev/github.com/vapstack/x/proc) for reference
