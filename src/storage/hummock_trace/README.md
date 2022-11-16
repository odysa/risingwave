# Hummock trace and replay

## Tracing


### Risedev

Put this env variable in `risedev-components.user.env`
```toml
ENABLE_hm_trace=true
```
It makes `risingdev` put flag `hm-trace` in env variables.

You can also config written log path
```toml
hm-trace_PATH=".trace/hummock.ht"
```

Then running any risedev commands traces storage operations to the log file.

### CLI
If you wish to manually run `cargo` rather than `risedev`, set the env variable to enable tracing.
```
RUSTFLAGS="--cfg hm-trace --cfg tokio_unstable"
```
We must manually enable `tokio_unstable` because extra flag sources are mutually exclusive. If we provide this variable, cargo will not evaluate `build.rustflags` in `.cargo/config.toml`

For example, to start a traced playground

```
RUSTFLAGS="--cfg hm-trace --cfg tokio_unstable" cargo run --bin risingwave playground
```

### Development
It's recommended to add `--cfg hm-trace` flag to `.cargo/config.toml` for development.
Example:
```toml
[target.'cfg(all())']
rustflags = [
  "--cfg",
  "hm-trace"
]
```

## Replay

### Config

Replaying requires the complete object storage from tracing. Please configure you object storage in your config file and make sure data remain in object storage.

Example:
```toml
[storage]
data_directory = "hummock_001"
local_object_store = "minio://hummockadmin:hummockadmin@host/hummock001"
```

### Sstable Offset hacking

In `src/storage/hummock/compactor/mod.rs` function `open_builder`, set
```rust
let table_id = self.sstable_id_manager.get_new_sst_id().await? + 2147483647; // add a large enough offset
```

Otherwise, replaying will leave object storage dirty and make version updates fail.

### Run Replay

Default storage config file, it uses `src/config/risingwave.toml`
```
cargo run --package risingwave_hummock_test --bin replay -- --path <your-path-to-log-file>
```

Customized config file
```
cargo run --package risingwave_hummock_test --bin replay -- --path <your-path-to-log-file> --config <your-path-to-config>
```