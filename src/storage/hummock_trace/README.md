# Hummock trace and replay

## Tracing

### Config
In the `risedev.yml`, we must disable the vacuum of the compactor.
```yml
  # Interval of GC stale metadata and SST
  vacuum-interval-sec: 30000000 // a very large number
```
Otherwise, the compactor may remove data from the object storage.
### Risedev
We can run `risedev configure` to make a env file.

Put env variables in `risedev-components.user.env`
```toml
# Path of log file
HM_TRACE_PATH=".trace/hummock.ht"
# Runtime tracing flag
USE_HM_TRACE=true
# Decide whether to compile it
ENABLE_HM_TRACE=true
```
It makes `risingdev` put flag `hm_trace` in env variables `RUSTFLAGS`.

Then running any risedev commands traces storage operations to the log file.

### CLI
If you wish to manually run `cargo` rather than `risedev`, set the env variable to enable tracing.
```
RUSTFLAGS="--cfg hm_trace --cfg tokio_unstable"
```
We must manually enable `tokio_unstable` because extra flag sources are mutually exclusive. If we provide this variable, cargo will not evaluate `build.rustflags` in `.cargo/config.toml`

For example, to start a traced playground

```
RUSTFLAGS="--cfg hm_trace --cfg tokio_unstable" cargo run --bin risingwave playground
```

### Development
It's recommended to add `--cfg hm_trace` flag to `.cargo/config.toml` for development since Rust may compile everything again if we set RUSTFLAGS.

Example:
```toml
[target.'cfg(all())']
rustflags = [
  "--cfg",
  "hm_trace"
]
```

If we set the flag in root `Cargo.toml`, we don't need to set the env variable.

## Replay

### Config

Replaying requires the complete object storage from tracing. Please make sure data remain in object storage.

### Run Replay

Default storage config file, it uses `src/config/risingwave.toml`
```
cargo run --package risingwave_hummock_test --bin replay --
--path <your-path-to-log-file>
--object-storage <your-object-storage>
```

Customized config file
```
cargo run --package risingwave_hummock_test --bin replay --
--path <your-path-to-log-file>
--config <your-path-to-config>
--object-storage <your-object-storage>
```