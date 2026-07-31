/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Launcher for embedded [seekdb](https://github.com/oceanbase/seekdb),
//! designed to pair with [`mysql_async`].
//!
//! [`open`] goes through the C driver (`libseekdb`): it spawns or attaches to
//! a server process rooted at `db_dir` and holds the lock that keeps it
//! alive. [`Seekdb::connect`] then wraps a plain [`mysql_async::Conn`]
//! dialed at the instance's unix socket ([`Seekdb::sock_path`]) into
//! [`Conn`], which derefs to the full `mysql_async` API:
//!
//! ```no_run
//! use seekdb_async::prelude::*;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let db = seekdb_async::open("./my.db").await?;
//! let mut conn = db.connect(Some("test")).await?;
//! let one: Option<i64> = conn.query_first("SELECT 1").await?;
//! conn.disconnect().await?;
//! drop(db);
//! # Ok(())
//! # }
//! ```
//!
//! [`Conn::open`] is a one-step alternative: give it a `db_dir` and get a
//! connected [`Conn`] directly, with one shared instance handle per
//! `db_dir` in this process (released when the last such `Conn` goes away).
//!
//! # Server lifetime
//!
//! The server exits by itself once the last hold on its `db_dir` — in any
//! process, including C and Python clients sharing the same directory — is
//! released. A [`Conn`] holds the [`Seekdb`] it came from, so the server
//! cannot shut down under connections made through
//! [`connect`](Seekdb::connect)/[`connect_opts`](Seekdb::connect_opts) —
//! drop order never matters there. Raw `mysql_async` connections or pools
//! you build directly from [`Seekdb::opts`] carry no such hold: keep the
//! `Seekdb` alive yourself while they are in use. The server can still die
//! externally (it shares the host's process group, so a terminal Ctrl+C
//! kills it; crashes too) — connections then fail with connection-reset /
//! broken-pipe / connection-refused errors, and the remedy is to reopen.
//!
//! # Session notes
//!
//! - Sessions default to the MySQL default `autocommit=1`. For the
//!   transactional behavior of the Python binding, add
//!   `.setup(vec!["SET autocommit=0".into()])` to the [`Seekdb::opts`]
//!   builder — `setup` commands are re-applied after pool connection resets.
//! - Right after [`open`] on a restarted `db_dir`, the server's user schema
//!   may lag readiness by a couple of seconds: connects can briefly fail
//!   with `1049 Unknown database` and first queries with `1146 Table
//!   doesn't exist`, and during startup the socket may transiently refuse.
//!   Retry briefly on those if you race a fresh open.
//!
//! # Constraints
//!
//! - POSIX only (an embedded server listens only on a unix socket).
//! - `db_dir` must be valid UTF-8 and short enough that
//!   `<db_dir>/run/sql.sock` fits the OS socket-path limit; [`open`] checks
//!   both up front.
//! - The socket is created with the spawning process's umask; a restrictive
//!   umask can make it unreachable for other-user clients of the same
//!   `db_dir`.
//! - [`open`] blocks (on a background thread) until the server accepts SQL,
//!   **without a timeout**, exactly like the C driver — first init runs the
//!   full bootstrap, and the future cannot cancel the underlying C call.
//!   Prefer `Runtime::shutdown_timeout` when tearing down the runtime.
//! - Each open that actually spawns a server leaves one bounded zombie
//!   process after that server later exits (the C driver never reaps its
//!   children).
//! - Building needs a `libseekdb` with the `seekdb` server binary next to
//!   it, found via `SEEKDB_LIB_DIR` or the repository's `build/` directory.

use std::collections::HashMap;
use std::ffi::CString;
use std::ops::{Deref, DerefMut};
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

pub use mysql_async;
pub use mysql_async::prelude;

mod ffi {
    use std::os::raw::{c_char, c_int, c_void};

    pub type SeekdbHandle = *mut c_void;

    pub const SEEKDB_SUCCESS: c_int = 0;

    extern "C" {
        pub fn seekdb_open(
            db_dir: *const c_char,
            parameters: *const *const c_char,
            out_handle: *mut SeekdbHandle,
        ) -> c_int;
        pub fn seekdb_close(handle: SeekdbHandle) -> c_int;
    }
}

#[cfg(target_os = "linux")]
const SUN_PATH_MAX: usize = 108;
#[cfg(not(target_os = "linux"))]
const SUN_PATH_MAX: usize = 104;
const SOCK_SUFFIX: &str = "/run/sql.sock";

#[derive(Debug)]
pub enum Error {
    Open { code: i32, db_dir: PathBuf },
    InvalidInput(String),
    Mysql(mysql_async::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Open { code, db_dir } => write!(
                f,
                "seekdb_open failed with code {code} for {}; server diagnostics (if any) are in {}",
                db_dir.display(),
                db_dir.join("log").join("seekdb.log").display()
            ),
            Error::InvalidInput(msg) => write!(f, "{msg}"),
            Error::Mysql(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Mysql(e) => Some(e),
            _ => None,
        }
    }
}

impl From<mysql_async::Error> for Error {
    fn from(e: mysql_async::Error) -> Self {
        Error::Mysql(e)
    }
}

struct Inner {
    raw: ffi::SeekdbHandle,
    db_dir: PathBuf,
    sock_path: PathBuf,
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

static SHARED: LazyLock<Mutex<HashMap<PathBuf, Weak<Inner>>>> = LazyLock::new(Default::default);

impl Drop for Inner {
    fn drop(&mut self) {
        if let Ok(mut map) = SHARED.lock() {
            if let Some(weak) = map.get(&self.db_dir) {
                if weak.upgrade().is_none() {
                    map.remove(&self.db_dir);
                }
            }
        }
        unsafe {
            ffi::seekdb_close(self.raw);
        }
    }
}

/// A running (or attached) embedded seekdb instance.
///
/// Cloning is cheap and shares the same handle. The handle is released once
/// the last clone **and every [`Conn`] created from it** are dropped; the
/// server exits once no handle in any process remains.
#[derive(Clone)]
pub struct Seekdb {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Seekdb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Seekdb")
            .field("db_dir", &self.inner.db_dir)
            .finish()
    }
}

impl Seekdb {
    pub fn db_dir(&self) -> &Path {
        &self.inner.db_dir
    }

    /// The instance's unix socket: `<db_dir>/run/sql.sock`. This is the same
    /// path the C driver itself dials; it exists and accepts connections by
    /// the time [`open`] returns.
    pub fn sock_path(&self) -> &Path {
        &self.inner.sock_path
    }

    /// An [`mysql_async::OptsBuilder`] pre-filled with the socket path and
    /// user `root` (empty password) — extend it (`setup`, pool options, …)
    /// and pass it to [`connect_opts`](Self::connect_opts) or
    /// [`mysql_async::Pool::new`]. Note connections built from these opts
    /// **without** going through [`connect`](Self::connect)/`connect_opts`
    /// do not keep the instance alive — keep the `Seekdb` around yourself.
    pub fn opts(&self, db_name: Option<&str>) -> mysql_async::OptsBuilder {
        mysql_async::OptsBuilder::default()
            .socket(Some(
                self.inner
                    .sock_path
                    .to_str()
                    .expect("validated UTF-8 in open")
                    .to_string(),
            ))
            .user(Some("root".to_string()))
            .db_name(db_name.map(str::to_string))
    }

    /// Connects with the default [`opts`](Self::opts). The returned [`Conn`]
    /// keeps this instance alive. `db_name` is the session's default
    /// database (the MySQL handshake field, like `mysql -D`); `None` means
    /// no default — qualify table names or `USE` one later.
    pub async fn connect(&self, db_name: Option<&str>) -> Result<Conn> {
        self.connect_opts(self.opts(db_name)).await
    }

    /// Connects with caller-customized opts (typically built from
    /// [`opts`](Self::opts), e.g. with a `setup` command added).
    pub async fn connect_opts(&self, opts: impl Into<mysql_async::Opts>) -> Result<Conn> {
        let conn = mysql_async::Conn::new(opts.into()).await?;
        Ok(Conn {
            conn,
            _db: self.inner.clone(),
        })
    }
}

/// A SQL session: [`mysql_async::Conn`] plus a hold on the [`Seekdb`]
/// instance it belongs to, so the server cannot shut down under a live
/// connection. The full `mysql_async` API
/// ([`prelude::Queryable`](prelude::Queryable) etc.) is available through
/// deref.
pub struct Conn {
    conn: mysql_async::Conn,
    _db: Arc<Inner>,
}

impl Conn {
    /// One-step connect: open (or reuse) the instance at `db_dir`, then
    /// connect a session with `db_name` as the default database.
    ///
    /// Instances opened this way are shared per process: all `Conn::open`
    /// calls for the same `db_dir` (same absolute path) reuse one underlying
    /// `seekdb_open` handle. When the last such [`Conn`] is dropped the
    /// handle is released — the server exits if no other client (in any
    /// process) still holds it — and the next `Conn::open` opens afresh.
    /// Handles from [`crate::open`] are independent and never shared with
    /// this registry.
    pub async fn open(db_dir: impl AsRef<Path>, db_name: Option<&str>) -> Result<Conn> {
        let inner = shared_inner(db_dir.as_ref()).await?;
        let db = Seekdb { inner };
        db.connect(db_name).await
    }

    pub async fn disconnect(self) -> Result<()> {
        self.conn.disconnect().await.map_err(Into::into)
    }

    /// The wrapped connection, for APIs that want the raw type by value;
    /// this drops the hold on the instance.
    pub fn into_inner(self) -> mysql_async::Conn {
        self.conn
    }
}

impl Deref for Conn {
    type Target = mysql_async::Conn;
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for Conn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.conn.fmt(f)
    }
}

/// Opens the instance at `db_dir` with default parameters. See
/// [`open_with`] and the crate docs for blocking behavior and constraints.
pub async fn open(db_dir: impl AsRef<Path>) -> Result<Seekdb> {
    open_with(db_dir, &[]).await
}

/// Opens with server parameters (e.g. `[("memory_limit", "2G")]`), which
/// take effect **only when `db_dir` is initialized for the first time**
/// (persisted afterwards; ignored on restart so persisted values survive).
/// Unless overridden, first init seeds `memory_limit=1G` and
/// `log_disk_size=2G`.
///
/// The C driver's reserved `port` key is rejected: an embedded server has no
/// TCP listener, and passing `port` would make the C driver poll a TCP
/// endpoint that never comes up, hanging forever.
pub async fn open_with(db_dir: impl AsRef<Path>, params: &[(&str, &str)]) -> Result<Seekdb> {
    for (key, _) in params {
        if key.is_empty() {
            return Err(Error::InvalidInput(
                "server parameter keys must be non-empty".into(),
            ));
        }
        if *key == "port" {
            return Err(Error::InvalidInput(
                "parameter \"port\" is not supported: an embedded seekdb server has no TCP \
                 listener, and this client always connects via <db_dir>/run/sql.sock"
                    .into(),
            ));
        }
    }
    let abs = absolutize(db_dir.as_ref())?;
    let owned: Vec<(String, String)> = params
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let inner = open_inner(abs, owned).await?;
    Ok(Seekdb { inner })
}

async fn open_inner(abs: PathBuf, params: Vec<(String, String)>) -> Result<Arc<Inner>> {
    let sock_path = PathBuf::from(format!(
        "{}{}",
        abs.to_str().expect("validated in absolutize"),
        SOCK_SUFFIX
    ));
    let abs_for_open = abs.clone();
    let raw = tokio::task::spawn_blocking(move || open_blocking(&abs_for_open, &params))
        .await
        .unwrap_or_else(|join_err| {
            if join_err.is_panic() {
                std::panic::resume_unwind(join_err.into_panic())
            } else {
                panic!("tokio runtime shut down while seekdb_open was in flight")
            }
        })?;
    Ok(Arc::new(Inner {
        raw: raw.0,
        db_dir: abs,
        sock_path,
    }))
}

async fn shared_inner(db_dir: &Path) -> Result<Arc<Inner>> {
    let abs = absolutize(db_dir)?;
    {
        let map = SHARED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = map.get(&abs).and_then(Weak::upgrade) {
            return Ok(existing);
        }
    }
    let fresh = open_inner(abs.clone(), Vec::new()).await?;
    let mut map = SHARED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = map.get(&abs).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    map.insert(abs, Arc::downgrade(&fresh));
    Ok(fresh)
}

struct RawHandle(ffi::SeekdbHandle);
unsafe impl Send for RawHandle {}

fn open_blocking(db_dir: &Path, params: &[(String, String)]) -> Result<RawHandle> {
    let c_dir = CString::new(db_dir.as_os_str().as_bytes())
        .map_err(|_| Error::InvalidInput("db_dir contains an interior NUL byte".into()))?;
    let mut c_params: Vec<CString> = Vec::with_capacity(params.len() * 2);
    for (key, value) in params {
        for part in [key, value] {
            c_params.push(CString::new(part.as_bytes()).map_err(|_| {
                Error::InvalidInput(format!("parameter {key:?} contains an interior NUL byte"))
            })?);
        }
    }
    let mut ptrs: Vec<*const c_char> = c_params.iter().map(|s| s.as_ptr()).collect();
    ptrs.push(std::ptr::null());
    let params_ptr = if params.is_empty() {
        std::ptr::null()
    } else {
        ptrs.as_ptr()
    };

    let mut handle: ffi::SeekdbHandle = std::ptr::null_mut();
    let rc = unsafe { ffi::seekdb_open(c_dir.as_ptr(), params_ptr, &mut handle) };

    if rc != ffi::SEEKDB_SUCCESS {
        return Err(Error::Open {
            code: rc,
            db_dir: db_dir.to_path_buf(),
        });
    }
    Ok(RawHandle(handle))
}

fn absolutize(db_dir: &Path) -> Result<PathBuf> {
    let abs = if db_dir.is_absolute() {
        db_dir.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| Error::InvalidInput(format!("cannot resolve current dir: {e}")))?
            .join(db_dir)
    };
    if abs.to_str().is_none() {
        return Err(Error::InvalidInput(
            "db_dir must be valid UTF-8: the SQL socket path is passed to mysql_async as a String"
                .into(),
        ));
    }
    let sock_len = abs.as_os_str().len() + SOCK_SUFFIX.len();
    if sock_len >= SUN_PATH_MAX {
        return Err(Error::InvalidInput(format!(
            "db_dir {} is too long: {}{} would be {sock_len} bytes, over this platform's unix \
             socket path limit ({SUN_PATH_MAX}); use a shorter db_dir",
            abs.display(),
            abs.display(),
            SOCK_SUFFIX
        )));
    }
    Ok(abs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlong_db_dir_is_rejected() {
        let long = PathBuf::from(format!("/{}", "d".repeat(120)));
        assert!(absolutize(&long)
            .unwrap_err()
            .to_string()
            .contains("too long"));
    }

    #[test]
    fn non_utf8_db_dir_is_rejected() {
        use std::ffi::OsStr;
        let bad = PathBuf::from(OsStr::from_bytes(b"/tmp/\xff"));
        assert!(absolutize(&bad).unwrap_err().to_string().contains("UTF-8"));
    }

    #[test]
    fn short_db_dir_passes() {
        assert!(absolutize(Path::new("/tmp/x")).is_ok());
    }

    #[tokio::test]
    async fn port_parameter_is_rejected() {
        let err = open_with("/tmp/x", &[("port", "3306")]).await.unwrap_err();
        assert!(err.to_string().contains("port"));
    }

    #[tokio::test]
    async fn empty_key_is_rejected() {
        assert!(open_with("/tmp/x", &[("", "v")]).await.is_err());
    }
}
