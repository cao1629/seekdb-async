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

use seekdb_async::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./seekdb_example.db".to_string());
    let db = seekdb_async::open(&db_dir).await?;
    let mut conn = db
        .connect_opts(
            db.opts(Some("test"))
                .setup(vec!["SET autocommit=0".to_string()]),
        )
        .await?;

    conn.query_drop("DROP TABLE IF EXISTS doc_table").await?;
    conn.query_drop(
        "CREATE TABLE doc_table(c1 INT, vector VECTOR(3), query VARCHAR(255), content VARCHAR(255), \
         VECTOR INDEX idx1(vector) WITH (distance=l2, type=hnsw, lib=vsag), \
         FULLTEXT idx2(query), FULLTEXT idx3(content))",
    )
    .await?;
    conn.query_drop(
        "INSERT INTO doc_table VALUES \
         (1, '[1,2,3]', 'hello world', 'oceanbase Elasticsearch database'), \
         (2, '[1,2,1]', 'hello world, what is your name', 'oceanbase mysql database'), \
         (3, '[1,1,1]', 'hello world, how are you', 'oceanbase oracle database'), \
         (4, '[1,3,1]', 'real world, where are you from', 'postgres oracle database'), \
         (5, '[1,3,2]', 'real world, how old are you', 'redis oracle database'), \
         (6, '[2,1,1]', 'hello world, where are you from', 'starrocks oceanbase database')",
    )
    .await?;
    conn.query_drop("COMMIT").await?;

    conn.query_drop(
        r#"SET @parm = '{
      "query": {"bool": {"should": [
        {"match": {"query": "hi hello"}},
        {"match": {"content": "oceanbase mysql"}}
      ]}},
      "knn": {"field": "vector", "k": 5, "query_vector": [1,2,3]},
      "_source": ["query", "content", "_keyword_score", "_semantic_score"]
    }'"#,
    )
    .await?;
    conn.query_drop("COMMIT").await?;

    let result: Option<String> = conn
        .query_first("SELECT json_pretty(DBMS_HYBRID_SEARCH.SEARCH('doc_table', @parm))")
        .await?;
    println!("{}", result.unwrap_or_default());

    conn.disconnect().await?;
    Ok(())
}
