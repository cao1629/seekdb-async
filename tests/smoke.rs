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

use seekdb_async::mysql_async::{OptsBuilder, Pool};
use seekdb_async::prelude::*;
use seekdb_async::Conn;
use std::path::PathBuf;

fn test_db_dir(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    dir
}

#[tokio::test]
async fn select_one() {
    let dir = test_db_dir("smoke5");
    let mut conn = Conn::open(&dir, None).await.unwrap();

    let one: Option<i64> = conn.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));

    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn open_query_txn_roundtrip() {
    let dir = test_db_dir("smoke1");
    let mut conn = Conn::open(&dir, Some("test")).await.unwrap();
    conn.query_drop("SET autocommit=0").await.unwrap();

    let one: Option<i64> = conn.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
    let ac: Option<i64> = conn.query_first("SELECT @@autocommit").await.unwrap();
    assert_eq!(ac, Some(0));

    conn.query_drop("DROP TABLE IF EXISTS smoke_t")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE smoke_t (id INT PRIMARY KEY, v VARCHAR(32))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO smoke_t VALUES (1,'a'),(2,'b')")
        .await
        .unwrap();
    conn.query_drop("ROLLBACK").await.unwrap();
    let n: Option<i64> = conn
        .query_first("SELECT COUNT(*) FROM smoke_t")
        .await
        .unwrap();
    assert_eq!(n, Some(0));

    conn.exec_drop("INSERT INTO smoke_t VALUES (?,?)", (3, "c"))
        .await
        .unwrap();
    conn.query_drop("COMMIT").await.unwrap();
    let rows: Vec<(i64, String)> = conn.query("SELECT id, v FROM smoke_t").await.unwrap();
    assert_eq!(rows, vec![(3, "c".to_string())]);

    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn pool_and_sock_path() {
    let dir = test_db_dir("smoke2");
    let keeper = Conn::open(&dir, Some("test")).await.unwrap();
    let sock_path = PathBuf::from(keeper.opts().socket().unwrap());

    assert!(sock_path.ends_with("run/sql.sock"));
    assert!(sock_path.exists());
    assert_eq!(dir.join("run/sql.sock"), sock_path);

    let pool = Pool::new(
        OptsBuilder::from_opts(keeper.opts().clone()).setup(vec!["SET autocommit=0".to_string()]),
    );
    let mut c = pool.get_conn().await.unwrap();
    let ac: Option<i64> = c.query_first("SELECT @@autocommit").await.unwrap();
    assert_eq!(ac, Some(0));
    drop(c);
    let mut c = pool.get_conn().await.unwrap();
    let ac: Option<i64> = c.query_first("SELECT @@autocommit").await.unwrap();
    assert_eq!(ac, Some(0));
    drop(c);
    pool.disconnect().await.unwrap();
    keeper.disconnect().await.unwrap();
}

#[tokio::test]
async fn two_connections_share_data() {
    let dir = test_db_dir("smoke6");

    let mut writer = Conn::open(&dir, Some("test")).await.unwrap();
    let mut reader = Conn::open(&dir, Some("test")).await.unwrap();

    writer
        .query_drop("DROP TABLE IF EXISTS one_col")
        .await
        .unwrap();
    writer
        .query_drop("CREATE TABLE one_col (v INT)")
        .await
        .unwrap();
    writer
        .query_drop("INSERT INTO one_col VALUES (42)")
        .await
        .unwrap();

    let got: Option<i64> = reader.query_first("SELECT v FROM one_col").await.unwrap();
    assert_eq!(got, Some(42));

    writer.disconnect().await.unwrap();
    reader.disconnect().await.unwrap();
}

#[tokio::test]
async fn two_conn_opens_share_data() {
    let dir = test_db_dir("smoke8");

    let mut writer = Conn::open(&dir, Some("test")).await.unwrap();
    let mut reader = Conn::open(&dir, Some("test")).await.unwrap();

    writer
        .query_drop("DROP TABLE IF EXISTS cross_handle")
        .await
        .unwrap();
    writer
        .query_drop("CREATE TABLE cross_handle (v INT)")
        .await
        .unwrap();
    writer
        .query_drop("INSERT INTO cross_handle VALUES (7)")
        .await
        .unwrap();

    let got: Option<i64> = reader
        .query_first("SELECT v FROM cross_handle")
        .await
        .unwrap();
    assert_eq!(got, Some(7));

    writer.disconnect().await.unwrap();
    reader.disconnect().await.unwrap();
}

#[tokio::test]
async fn conn_open_shares_one_handle_per_dir() {
    let dir = test_db_dir("smoke9");

    let mut a = seekdb_async::Conn::open(&dir, Some("test")).await.unwrap();
    let mut b = seekdb_async::Conn::open(&dir, Some("test")).await.unwrap();

    a.query_drop("DROP TABLE IF EXISTS shared_t").await.unwrap();
    a.query_drop("CREATE TABLE shared_t (v INT)").await.unwrap();
    a.query_drop("INSERT INTO shared_t VALUES (9)")
        .await
        .unwrap();
    let got: Option<i64> = b.query_first("SELECT v FROM shared_t").await.unwrap();
    assert_eq!(got, Some(9));

    a.disconnect().await.unwrap();
    let still: Option<i64> = b.query_first("SELECT 1").await.unwrap();
    assert_eq!(still, Some(1));

    b.disconnect().await.unwrap();

    let mut reopened = seekdb_async::Conn::open(&dir, Some("test")).await;
    for _ in 0..50 {
        if reopened.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        reopened = seekdb_async::Conn::open(&dir, Some("test")).await;
    }
    let mut c = reopened.unwrap();
    let back: Option<i64> = c.query_first("SELECT v FROM shared_t").await.unwrap();
    assert_eq!(back, Some(9));
    c.disconnect().await.unwrap();
}

#[tokio::test]
async fn conn_keeps_instance_alive() {
    let dir = test_db_dir("smoke3");
    let mut conn = Conn::open(&dir, None).await.unwrap();

    let one: Option<i64> = conn.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
    conn.disconnect().await.unwrap();
}

#[tokio::test]
async fn conn_open_twice_then_disconnect_in_order() {
    let dir = test_db_dir("smoke7");
    let conn1 = Conn::open(&dir, None).await.unwrap();
    let conn2 = Conn::open(&dir, None).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    conn1.disconnect().await.unwrap();
    println!("disconnected conn1");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    conn2.disconnect().await.unwrap();
    println!("disconnected conn2");
}

#[tokio::test]
async fn two_conns_same_dir() {
    let dir = test_db_dir("smoke4");
    let conn1 = Conn::open(&dir, None).await.unwrap();
    let mut conn2 = Conn::open(&dir, None).await.unwrap();

    conn1.disconnect().await.unwrap();

    let one: Option<i64> = conn2.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));

    conn2.disconnect().await.unwrap();
}
