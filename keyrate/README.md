## keyrate 

A `rate.Limiter` per key with bounded retention.

`KeyLimiter` lazily creates a `golang.org/x/time/rate.Limiter` for each key and
keeps limiters in an internal two-generation map to avoid unbounded growth 
when the set of keys is large and mostly ephemeral.

Limiters are created on demand and are retained for approximately two swap
intervals. Swap interval is equal to the duration provided to `NewKeyLimiter`,
but never less than 5 seconds.

```go
l := keyrate.NewKeyLimiter[string](5*time.Minute, 5, 1<<12)

if !l.Allow("12345") {
    // ...
}
```
