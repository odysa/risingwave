// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(madsim)]

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use madsim::time::sleep;
use risingwave_simulation_scale::cluster::{Cluster, Configuration};
use risingwave_simulation_scale::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation_scale::utils::AssertResult;

const SELECT: &str = "select * from mv1 order by v1;";

#[madsim::test]
async fn test_dynamic_filter() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::default()).await?;

    cluster.run("create table t1 (v1 int);").await?;
    cluster.run("create table t2 (v2 int);").await?;
    cluster.run("create materialized view mv1 as with max_v2 as (select max(v2) max from t2) select v1 from t1, max_v2 where v1 > max;").await?;
    cluster.run("insert into t1 values (1), (2), (3)").await?;
    cluster.run("flush").await?;
    sleep(Duration::from_secs(5)).await;

    let dynamic_filter_fragment = cluster
        .locate_one_fragment(vec![identity_contains("dynamicFilter")])
        .await?;

    let materialize_fragments = cluster
        .locate_fragments(vec![identity_contains("materialize")])
        .await?;

    let upstream_fragment_ids: HashSet<_> = dynamic_filter_fragment
        .inner
        .upstream_fragment_ids
        .iter()
        .collect();

    let fragment = materialize_fragments
        .iter()
        .find(|fragment| upstream_fragment_ids.contains(&fragment.id()))
        .unwrap();

    let id = fragment.id();

    cluster.reschedule(format!("{id}-[1,2,3]")).await?;
    sleep(Duration::from_secs(3)).await;

    cluster.run(SELECT).await?.assert_result_eq("");
    cluster.run("insert into t2 values (0)").await?;
    cluster.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    cluster.run(SELECT).await?.assert_result_eq("1\n2\n3");
    // 1
    // 2
    // 3

    cluster.reschedule(format!("{id}-[4,5]+[1,2,3]")).await?;
    sleep(Duration::from_secs(3)).await;
    cluster.run(SELECT).await?.assert_result_eq("1\n2\n3");

    cluster.run("insert into t2 values (2)").await?;
    cluster.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    cluster.run(SELECT).await?.assert_result_eq("3");
    // 3

    cluster.reschedule(format!("{id}-[1,2,3]+[4,5]")).await?;
    sleep(Duration::from_secs(3)).await;
    cluster.run(SELECT).await?.assert_result_eq("3");

    cluster.run("update t2 set v2 = 1 where v2 = 2").await?;
    cluster.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    cluster.run(SELECT).await?.assert_result_eq("2\n3");
    // 2
    // 3
    //
    cluster.reschedule(format!("{id}+[1,2,3]")).await?;
    sleep(Duration::from_secs(3)).await;
    cluster.run(SELECT).await?.assert_result_eq("2\n3");

    cluster.run("delete from t2 where true").await?;
    cluster.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    cluster.run(SELECT).await?.assert_result_eq("");

    cluster.reschedule(format!("{id}-[1]")).await?;
    sleep(Duration::from_secs(3)).await;
    cluster.run(SELECT).await?.assert_result_eq("");

    cluster.run("insert into t2 values (1)").await?;
    cluster.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    cluster.run(SELECT).await?.assert_result_eq("2\n3");

    Ok(())
}
