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

use risingwave_common::catalog::Schema;

use super::{stream, EqJoinPredicate};
use crate::optimizer::optimizer_context::OptimizerContextRef;

pub mod dynamic_filter;
pub use dynamic_filter::*;
mod hop_window;
pub use hop_window::*;
mod agg;
pub use agg::*;
mod project_set;
pub use project_set::*;
mod join;
pub use join::*;
mod project;
pub use project::*;
mod filter;
pub use filter::*;
mod expand;
pub use expand::*;
mod source;
pub use source::*;
mod scan;
pub use scan::*;
mod union;
pub use union::*;
mod top_n;
pub use top_n::*;
mod share;
pub use share::*;

pub trait GenericPlanRef {
    fn schema(&self) -> &Schema;
    fn logical_pk(&self) -> &[usize];
    fn ctx(&self) -> OptimizerContextRef;
}

pub trait GenericPlanNode {
    fn schema(&self) -> Schema;
    fn logical_pk(&self) -> Option<Vec<usize>>;
    fn ctx(&self) -> OptimizerContextRef;
}
