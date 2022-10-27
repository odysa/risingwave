# Hummock trace and replay

## Tracing


### risedev

Put this env variable in `risedev-components.user.env`
```
ENABLE_HM_TRACE=true
```

Then use `risedev`

### CLI
To enable tracing, set env variable
```
RUSTFLAGS="--cfg hm_trace --cfg tokio_unstable"
```
We must manually enable `tokio_unstable` because extra flag sources are mutually exclusive. If we provide this variable, cargo will not evaluate `build.rustflags` in `.cargo/config.toml`

For example, to start a traced playground

```
RUSTFLAGS="--cfg hm_trace --cfg tokio_unstable" cargo run --bin risingwave playground
```