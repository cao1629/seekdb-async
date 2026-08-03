# seekdb-async

Async Rust client for embedded [seekdb](https://github.com/oceanbase/seekdb), built on [`mysql_async`](https://crates.io/crates/mysql_async).

The lifecycle goes through seekdb's C driver (`libseekdb`): opening a database directory spawns (or attaches to) a seekdb server process and holds the lock that keeps it alive. Everything after that is plain `mysql_async` speaking MySQL protocol over the instance's unix socket.

```rust
let mut conn = seekdb_async::Conn::open("./my.db", Some("test")).await?;

conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(32))").await?;
conn.exec_drop("INSERT INTO t VALUES (?, ?)", (1, "hello")).await?;
let rows: Vec<(i64, String)> = conn.query("SELECT id, v FROM t").await?;

conn.disconnect().await?;
```

`Conn::open` shares one instance handle per `db_dir` within the process; when the last `Conn` for a directory is dropped, the handle is released and the server exits on its own once no client (in any process) remains.

Connection creation mirrors `mysql_async`: `Conn::new` and `Pool::new` take `impl Into<Opts>` — an `OptsBuilder` or a `seekdb://` URL (`seekdb://<db_dir>?db_name=<db>&<param>=<value>`, where extra query pairs are first-init server parameters):

```rust
let conn = seekdb_async::Conn::new("seekdb://./my.db?db_name=test&memory_limit=2G").await?;

let pool = seekdb_async::Pool::new(
    seekdb_async::OptsBuilder::default().db_dir("./my.db").db_name(Some("test")),
);
let conn = pool.get_conn().await?;
```

`Pool::new` is lazy and cheap, clones share the pool, and pooled connections keep the instance alive too.

## Requirements

- POSIX (Linux / macOS). The embedded server is reachable only over a unix socket.
- A `libseekdb` shared library with the `seekdb` server binary **next to it**. The build script obtains them automatically (see below), or you can build both from [seekdb-bindings](https://github.com/cao1629/seekdb-bindings):

  ```sh
  git clone https://github.com/cao1629/seekdb-bindings.git
  cd seekdb-bindings
  git submodule update --init deps/mariadb-connector-c deps/googletest
  export SEEKDB_BIN=/path/to/seekdb   # a built seekdb server binary
  cmake -S . -B build                 # macOS: add -DWITH_EXTERNAL_ZLIB=YES
  cmake --build build --target seekdb
  ```

## Building

The build script locates the runtime in this order:

1. `SEEKDB_LIB_DIR` environment variable — a directory containing `libseekdb.{dylib,so}` and `seekdb`;
2. **download**: a prebuilt runtime archive is fetched from this repo's [releases](https://github.com/cao1629/seekdb-async/releases) and unpacked into `OUT_DIR` (currently `aarch64-apple-darwin` only). The download honors `HTTPS_PROXY`/`HTTP_PROXY` environment variables.

So on a supported platform a plain `cargo build` / `cargo test` works with no setup. To use your own artifacts instead:

```sh
SEEKDB_LIB_DIR=/path/to/dir-with-libseekdb cargo build
```

At runtime the dynamic loader must find `libseekdb` too. `cargo test` / `cargo run` inside this repo work out of the box (an rpath is baked into the test binaries). Downstream applications add one line to their own `build.rs`:

```rust
println!("cargo:rustc-link-arg=-Wl,-rpath,{}", std::env::var("DEP_SEEKDB_LIB_DIR").unwrap());
```

or ship `libseekdb` + `seekdb` next to the application binary with an `@loader_path` / `$ORIGIN` rpath.

## License

Apache-2.0
