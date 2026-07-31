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

`Conn::open` shares one instance handle per `db_dir` within the process; when the last `Conn` for a directory is dropped, the handle is released and the server exits on its own once no client (in any process) remains. For explicit instance control use `seekdb_async::open` / `Seekdb`.

## Requirements

- POSIX (Linux / macOS). The embedded server is reachable only over a unix socket.
- A built `libseekdb` shared library with the `seekdb` server binary **next to it**. Get both by building [seekdb-bindings](https://github.com/cao1629/seekdb-bindings):

  ```sh
  git clone https://github.com/cao1629/seekdb-bindings.git
  cd seekdb-bindings
  git submodule update --init deps/mariadb-connector-c deps/googletest
  export SEEKDB_BIN=/path/to/seekdb   # a built seekdb server binary
  cmake -S . -B build                 # macOS: add -DWITH_EXTERNAL_ZLIB=YES
  cmake --build build --target seekdb
  ```

## Building

The build script locates `libseekdb` via the `SEEKDB_LIB_DIR` environment variable (falling back to a sibling `../seekdb-bindings/build` checkout):

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
