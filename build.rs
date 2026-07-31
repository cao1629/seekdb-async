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
    let in_repo_default =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../seekdb-bindings/build");
    println!(
        "cargo:rerun-if-changed={}",
        in_repo_default.join(lib_file_name()).display()
    );

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
    if found.is_none() && has_lib(&in_repo_default) {
        found = in_repo_default.canonicalize().ok();
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
                 Build it (cmake -S . -B build && cmake --build build --target seekdb) \
                 or point SEEKDB_LIB_DIR at a directory containing {} with the seekdb server binary beside it",
                lib_file_name()
            );
        }
    }
    println!("cargo:rustc-link-lib=dylib=seekdb");
}
