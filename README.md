# seekdb-async

Async Rust client for embedded [seekdb](https://github.com/oceanbase/seekdb), built on [`mysql_async`](https://crates.io/crates/mysql_async).

## Quick start

```toml
[dependencies]
seekdb-async = { git = "https://github.com/cao1629/seekdb-async" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use seekdb_async::prelude::*;
use seekdb_async::Conn;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Conn::open("./my.db", Some("test")).await?;
    let one: Option<i64> = conn.query_first("SELECT 1").await?;
    println!("{one:?}");
    conn.disconnect().await?;
    Ok(())
}
```

`cargo run` works with no setup on supported platforms — the build script downloads a prebuilt seekdb runtime automatically.

## Features

- Embedded lifecycle - opening a database directory spawns (or attaches to) a seekdb server process; it exits on its own when the last client is gone
- Full `mysql_async` API - `Conn` derefs to `mysql_async::Conn`: queries, prepared statements, transactions all work unchanged
- Aligned connection creation - `Conn::new` / `Pool::new` take an `OptsBuilder` or a `seekdb://` URL, like `mysql_async`
- Instance sharing - all connections to the same `db_dir` in a process share one server handle
- Self-contained deployment - ship the app binary with `libseekdb` and `seekdb` side by side

## Usage

Three equivalent ways to connect:

```rust
let conn = Conn::open("./my.db", Some("test")).await?;

let conn = Conn::new("seekdb://./my.db?db_name=test&memory_limit=2G").await?;

let conn = Conn::new(
    OptsBuilder::default()
        .db_dir("./my.db")
        .db_name(Some("test"))
        .parameter("memory_limit", "2G"),
)
.await?;
```

URL form: `seekdb://<db_dir>?db_name=<db>&<param>=<value>` — the path is the database directory; every query pair other than `db_name` is a server parameter, applied only when `db_dir` is initialized for the first time.

Pooling:

```rust
let pool = Pool::new("seekdb://./my.db?db_name=test");
let mut conn = pool.get_conn().await?;
```

`Pool::new` is lazy and cheap, clones share the pool, and pooled connections keep the instance alive. Await `pool.disconnect()` before shutting down the runtime.

Sessions use the MySQL default `autocommit=1`; run `SET autocommit=0` for the transactional behavior of the Python binding.

## Configuration

| Variable | Effect |
|---|---|
| `SEEKDB_LIB_DIR` | Build time: use this directory (containing `libseekdb.{dylib,so}` and `seekdb`) instead of downloading the runtime |
| `HTTPS_PROXY` / `HTTP_PROXY` | Proxy for the runtime download |

The runtime archive comes from this repo's [releases](https://github.com/cao1629/seekdb-async/releases) and unpacks into `OUT_DIR`. Prebuilt target: `aarch64-apple-darwin`.

## Deployment

The server binary is a separate process, so a distribution is a directory, not a single file:

```
myapp-dist/
├── myapp
├── libseekdb.dylib
└── seekdb
```

Bake a relative rpath in your app's `build.rs` so the directory is relocatable:

```rust
fn main() {
    if let Ok(dir) = std::env::var("DEP_SEEKDB_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-arg=-Wl,-rpath,@loader_path");
    #[cfg(not(target_os = "macos"))]
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
}
```

See [seekdb-hello](https://github.com/cao1629/seekdb-hello) for a complete example app with a packaging script.

## Limitations

- POSIX only (the embedded server listens on a unix socket)
- `db_dir` must be valid UTF-8 and short enough that `<db_dir>/run/sql.sock` fits the OS socket-path limit
- Opening blocks until the server accepts SQL, without a timeout; first init runs the full bootstrap
- Right after opening a previously initialized `db_dir`, the schema may lag for a couple of seconds (`1049` / `1146` errors) — retry briefly

## Development

```bash
# Build (downloads the runtime on first build)
cargo build

# Test (spawns real servers under target/tmp)
cargo test

# Lint
cargo clippy --all-targets -- -D warnings
```

## License

Apache-2.0
