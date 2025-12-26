## backoff

The package provides a simple exponential backoff with optional symmetric jitter.
The backoff grows from a minimum duration, is capped by a maximum duration, and
applies random jitter to avoid synchronized retries.

```go
// min, max, factor, jitter
b := backoff.New(time.Second, time.Minute, 3, 0.1)

for {
    err := doSomething()
    if err == nil {
        // ...
        return
    }
    
    select {
    case <-ctx.Done():
        return err
    case <-b.After(): // or <-time.After(b.Duration())
    }
}
```

See [GoDoc](https://pkg.go.dev/github.com/vapstack/x/backoff) for reference
