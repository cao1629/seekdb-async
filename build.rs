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

use std::path::{Path, PathBuf};

const RUNTIME_RELEASE_URL: &str =
    "https://github.com/cao1629/seekdb-async/releases/download/runtime-0.1.0";

fn lib_file_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "libseekdb.dylib"
    } else {
        "libseekdb.so"
    }
}

fn has_lib(dir: &Path) -> bool {
    dir.join(lib_file_name()).exists()
}

fn main() {
    println!("cargo:rerun-if-env-changed=SEEKDB_LIB_DIR");
    if std::env::var_os("DOCS_RS").is_some() {
        return;
    }

    let mut found: Option<PathBuf> = None;
    if let Some(dir) = std::env::var_os("SEEKDB_LIB_DIR").map(PathBuf::from) {
        println!(
            "cargo:rerun-if-changed={}",
            dir.join(lib_file_name()).display()
        );
        if has_lib(&dir) {
            found = dir.canonicalize().ok();
        } else {
            println!(
                "cargo:warning=SEEKDB_LIB_DIR={} does not contain {}",
                dir.display(),
                lib_file_name()
            );
        }
    }
    if found.is_none() {
        found = download_runtime();
    }

    match found {
        Some(dir) => {
            println!("cargo:rustc-link-search=native={}", dir.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dir.display());
            println!("cargo:lib_dir={}", dir.display());
        }
        None => {
            println!(
                "cargo:warning=libseekdb not found; `cargo build`/`cargo test` will fail at link time. \
                 Check the runtime download warnings above, or point SEEKDB_LIB_DIR at a directory \
                 containing {} with the seekdb server binary beside it",
                lib_file_name()
            );
        }
    }
    println!("cargo:rustc-link-lib=dylib=seekdb");
}

fn download_runtime() -> Option<PathBuf> {
    let target = std::env::var("TARGET").ok()?;
    let out = PathBuf::from(std::env::var_os("OUT_DIR")?).join("seekdb-runtime");
    if has_lib(&out) && out.join("seekdb").exists() {
        return Some(out);
    }
    let url = format!("{RUNTIME_RELEASE_URL}/seekdb-runtime-{target}.tar.gz");
    println!("cargo:warning=downloading seekdb runtime ({target}) from {url}");
    match fetch_unroll::Fetch::from(&url).unroll().to(&out) {
        Ok(()) => {
            if has_lib(&out) && out.join("seekdb").exists() {
                Some(out)
            } else {
                println!("cargo:warning=downloaded runtime archive is missing expected files");
                None
            }
        }
        Err(e) => {
            println!("cargo:warning=seekdb runtime download failed: {e}");
            None
        }
    }
}
