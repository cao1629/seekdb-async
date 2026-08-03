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
//! [`Conn::open`] goes through the C driver (`libseekdb`), spawning or
//! attaching to a server process rooted at `db_dir`, then returns a connected
//! [`Conn`] that derefs to the full [`mysql_async`] API:
//!
//! ```no_run
//! use seekdb_async::prelude::*;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let mut conn = seekdb_async::Conn::open("./my.db", Some("test")).await?;
//! let one: Option<i64> = conn.query_first("SELECT 1").await?;
//! conn.disconnect().await?;
//! # Ok(())
//! # }
//! ```
//!
//! The entry points mirror [`mysql_async`]: [`Conn::new`] and [`Pool::new`]
//! take `impl Into<Opts>` — an [`OptsBuilder`] or a `seekdb://` URL:
//!
//! ```no_run
//! # async fn demo() -> seekdb_async::Result<()> {
//! let conn = seekdb_async::Conn::new("seekdb://./my.db?db_name=test").await?;
//! let pool = seekdb_async::Pool::new(
//!     seekdb_async::OptsBuilder::default()
//!         .db_dir("./my.db")
//!         .db_name(Some("test")),
//! );
//! # Ok(()) }
//! ```
//!
//! All calls for the same `db_dir` share one instance handle in this process;
//! it is released when the last such [`Conn`] goes away.
//!
//! # Server lifetime
//!
//! The server exits by itself once the last hold on its `db_dir` — in any
//! process, including C and Python clients sharing the same directory — is
//! released. A [`Conn`] holds its underlying instance, so the server cannot
//! shut down while the connection is alive. Raw [`mysql_async::Conn`]s or
//! pools built from its options carry no such hold: keep the original
//! [`Conn`] alive while they are in use. The server can still die externally
//! (it shares the host's process group, so a terminal Ctrl+C kills it; crashes
//! too) — connections then fail with connection-reset / broken-pipe /
//! connection-refused errors, and the remedy is to reopen.
//!
//! # Session notes
//!
//! - Sessions default to the MySQL default `autocommit=1`. For the
//!   transactional behavior of the Python binding, execute
//!   `SET autocommit=0` after opening the connection. For pools, add it to the
//!   options' `setup` commands so it is re-applied after connection resets.
//! - Right after [`Conn::open`] on a restarted `db_dir`, the server's user schema
//!   may lag readiness by a couple of seconds: connects can briefly fail
//!   with `1049 Unknown database` and first queries with `1146 Table
//!   doesn't exist`, and during startup the socket may transiently refuse.
//!   Retry briefly on those if you race a fresh open.
//!
//! # Constraints
//!
//! - POSIX only (an embedded server listens only on a unix socket).
//! - `db_dir` must be valid UTF-8 and short enough that
//!   `<db_dir>/run/sql.sock` fits the OS socket-path limit; [`Conn::open`] checks
//!   both up front.
//! - The socket is created with the spawning process's umask; a restrictive
//!   umask can make it unreachable for other-user clients of the same
//!   `db_dir`.
//! - [`Conn::open`] blocks (on a background thread) until the server accepts SQL,
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

const DEFAULT_DB_DIR: &str = "./seekdb.db";

/// Connection options, mirroring [`mysql_async::Opts`]: build one with
/// [`OptsBuilder`] or parse one from a `seekdb://` URL, and pass it to
/// [`Conn::new`] or [`Pool::new`] (both take `impl Into<Opts>`).
///
/// URL form: `seekdb://<db_dir>?db_name=<db>&<param>=<value>…` — the path is
/// the database directory, `db_name` selects the session's default database,
/// and every other query pair is a first-init server parameter, e.g.
/// `seekdb://./my.db?db_name=test&memory_limit=2G`.
#[derive(Debug, Clone)]
pub struct Opts {
    db_dir: PathBuf,
    db_name: Option<String>,
    params: Vec<(String, String)>,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            db_dir: PathBuf::from(DEFAULT_DB_DIR),
            db_name: None,
            params: Vec::new(),
        }
    }
}

impl Opts {
    pub fn from_url(url: &str) -> Result<Opts> {
        let rest = url.strip_prefix("seekdb://").ok_or_else(|| {
            Error::InvalidInput(format!("seekdb URL must start with seekdb://, got {url:?}"))
        })?;
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        if path.is_empty() {
            return Err(Error::InvalidInput(
                "seekdb URL is missing the db_dir path".into(),
            ));
        }
        let mut opts = Opts {
            db_dir: PathBuf::from(path),
            db_name: None,
            params: Vec::new(),
        };
        for pair in query.split('&').filter(|s| !s.is_empty()) {
            let (key, value) = pair.split_once('=').ok_or_else(|| {
                Error::InvalidInput(format!("invalid query pair {pair:?} in seekdb URL"))
            })?;
            if key == "db_name" {
                opts.db_name = Some(value.to_string());
            } else {
                opts.params.push((key.to_string(), value.to_string()));
            }
        }
        Ok(opts)
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    pub fn db_name(&self) -> Option<&str> {
        self.db_name.as_deref()
    }

    pub fn parameters(&self) -> &[(String, String)] {
        &self.params
    }
}

impl From<&str> for Opts {
    /// Panics on an invalid URL, matching `mysql_async`'s `From<&str> for
    /// Opts`; use [`Opts::from_url`] for a fallible parse.
    fn from(url: &str) -> Opts {
        Opts::from_url(url).expect("invalid seekdb URL")
    }
}

impl From<String> for Opts {
    fn from(url: String) -> Opts {
        Opts::from(url.as_str())
    }
}

/// Builder for [`Opts`], mirroring [`mysql_async::OptsBuilder`].
#[derive(Debug, Clone, Default)]
pub struct OptsBuilder {
    opts: Opts,
}

impl OptsBuilder {
    /// The database directory (defaults to `./seekdb.db`, same as the other
    /// seekdb bindings).
    pub fn db_dir(mut self, db_dir: impl AsRef<Path>) -> Self {
        self.opts.db_dir = db_dir.as_ref().to_path_buf();
        self
    }

    /// The session's default database (the MySQL handshake field, like
    /// `mysql -D`); `None` means no default — qualify table names or `USE`
    /// one later.
    pub fn db_name<T: Into<String>>(mut self, db_name: Option<T>) -> Self {
        self.opts.db_name = db_name.map(Into::into);
        self
    }

    /// A server parameter, applied **only when `db_dir` is initialized for
    /// the first time** (persisted afterwards; ignored on restart). Unless
    /// overridden, first init seeds `memory_limit=1G` and `log_disk_size=2G`.
    /// The C driver's reserved `port` key is rejected at connect time.
    pub fn parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.opts.params.push((key.into(), value.into()));
        self
    }
}

impl From<OptsBuilder> for Opts {
    fn from(builder: OptsBuilder) -> Opts {
        builder.opts
    }
}

fn mysql_opts_for(inner: &Inner, db_name: Option<&str>) -> mysql_async::Opts {
    mysql_async::Opts::from(
        mysql_async::OptsBuilder::default()
            .socket(Some(
                inner
                    .sock_path
                    .to_str()
                    .expect("validated UTF-8 in open")
                    .to_string(),
            ))
            .user(Some("root".to_string()))
            .db_name(db_name.map(str::to_string)),
    )
}

/// A SQL session: [`mysql_async::Conn`] plus a hold on the [`Seekdb`]
/// instance it belongs to, so the server cannot shut down under a live
/// connection. The full `mysql_async` API
/// ([`prelude::Queryable`] etc.) is available through
/// deref.
pub struct Conn {
    conn: mysql_async::Conn,
    _db: Arc<Inner>,
}

impl Conn {
    /// Opens a connection from [`Opts`], an [`OptsBuilder`], or a
    /// `seekdb://` URL string — the analogue of [`mysql_async::Conn::new`]:
    ///
    /// ```no_run
    /// # async fn demo() -> seekdb_async::Result<()> {
    /// let conn = seekdb_async::Conn::new("seekdb://./my.db?db_name=test").await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The instance at `db_dir` is opened (or reused) first: all connections
    /// for the same `db_dir` (same absolute path) in this process share one
    /// underlying `seekdb_open` handle. When the last such [`Conn`] is
    /// dropped the handle is released — the server exits if no other client
    /// (in any process) still holds it — and the next connect opens afresh.
    /// Server parameters in the opts apply only if this call performs the
    /// first-ever initialization of `db_dir`.
    pub async fn new(opts: impl Into<Opts>) -> Result<Conn> {
        let opts = opts.into();
        validate_params(&opts.params)?;
        let inner = shared_inner(&opts.db_dir, opts.params.clone()).await?;
        let conn = mysql_async::Conn::new(mysql_opts_for(&inner, opts.db_name.as_deref())).await?;
        Ok(Conn { conn, _db: inner })
    }

    /// Shorthand for [`Conn::new`] with just a directory and default
    /// database.
    pub async fn open(db_dir: impl AsRef<Path>, db_name: Option<&str>) -> Result<Conn> {
        Conn::new(
            OptsBuilder::default()
                .db_dir(db_dir.as_ref())
                .db_name(db_name),
        )
        .await
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

/// A connection pool, mirroring [`mysql_async::Pool`]: `new` is cheap and
/// lazy (nothing is opened until the first [`get_conn`](Self::get_conn)),
/// cloning shares the pool, and every pooled [`Conn`] keeps the underlying
/// instance alive.
///
/// Await [`disconnect`](Self::disconnect) before shutting down the tokio
/// runtime, and drop all pooled connections first — like `mysql_async`, it
/// resolves only once every connection has been returned.
#[derive(Clone)]
pub struct Pool {
    state: Arc<PoolState>,
}

struct PoolState {
    opts: Opts,
    init: tokio::sync::OnceCell<(Arc<Inner>, mysql_async::Pool)>,
}

impl Pool {
    pub fn new(opts: impl Into<Opts>) -> Pool {
        Pool {
            state: Arc::new(PoolState {
                opts: opts.into(),
                init: tokio::sync::OnceCell::new(),
            }),
        }
    }

    async fn instance(&self) -> Result<&(Arc<Inner>, mysql_async::Pool)> {
        self.state
            .init
            .get_or_try_init(|| async {
                validate_params(&self.state.opts.params)?;
                let inner =
                    shared_inner(&self.state.opts.db_dir, self.state.opts.params.clone()).await?;
                let pool = mysql_async::Pool::new(mysql_opts_for(
                    &inner,
                    self.state.opts.db_name.as_deref(),
                ));
                Ok((inner, pool))
            })
            .await
    }

    pub async fn get_conn(&self) -> Result<Conn> {
        let (inner, pool) = self.instance().await?;
        let conn = pool.get_conn().await?;
        Ok(Conn {
            conn,
            _db: inner.clone(),
        })
    }

    pub async fn disconnect(self) -> Result<()> {
        if let Some((_, pool)) = self.state.init.get() {
            pool.clone().disconnect().await?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("db_dir", &self.state.opts.db_dir)
            .finish()
    }
}

fn validate_params(params: &[(String, String)]) -> Result<()> {
    for (key, _) in params {
        if key.is_empty() {
            return Err(Error::InvalidInput(
                "server parameter keys must be non-empty".into(),
            ));
        }
        if key == "port" {
            return Err(Error::InvalidInput(
                "parameter \"port\" is not supported: an embedded seekdb server has no TCP \
                 listener, and this client always connects via <db_dir>/run/sql.sock"
                    .into(),
            ));
        }
    }
    Ok(())
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

async fn shared_inner(db_dir: &Path, params: Vec<(String, String)>) -> Result<Arc<Inner>> {
    let abs = absolutize(db_dir)?;
    {
        let map = SHARED.lock().unwrap();
        if let Some(existing) = map.get(&abs).and_then(Weak::upgrade) {
            return Ok(existing);
        }
    }
    let fresh = open_inner(abs.clone(), params).await?;
    let mut map = SHARED.lock().unwrap();
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
        let err = Conn::new(
            OptsBuilder::default()
                .db_dir("/tmp/x")
                .parameter("port", "3306"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("port"));
    }

    #[tokio::test]
    async fn empty_key_is_rejected() {
        assert!(
            Conn::new(OptsBuilder::default().db_dir("/tmp/x").parameter("", "v"))
                .await
                .is_err()
        );
    }

    #[test]
    fn url_parses_path_db_name_and_params() {
        let opts = Opts::from_url("seekdb://./my.db?db_name=test&memory_limit=2G").unwrap();
        assert_eq!(opts.db_dir(), Path::new("./my.db"));
        assert_eq!(opts.db_name(), Some("test"));
        assert_eq!(
            opts.parameters(),
            &[("memory_limit".to_string(), "2G".to_string())]
        );
    }

    #[test]
    fn url_without_query() {
        let opts = Opts::from_url("seekdb:///data/db").unwrap();
        assert_eq!(opts.db_dir(), Path::new("/data/db"));
        assert_eq!(opts.db_name(), None);
        assert!(opts.parameters().is_empty());
    }

    #[test]
    fn url_rejects_wrong_scheme_and_empty_path() {
        assert!(Opts::from_url("mysql://x").is_err());
        assert!(Opts::from_url("seekdb://").is_err());
        assert!(Opts::from_url("seekdb://x?bad").is_err());
    }

    #[test]
    fn default_db_dir_matches_other_bindings() {
        assert_eq!(Opts::default().db_dir(), Path::new("./seekdb.db"));
    }
}
