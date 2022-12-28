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
mod enumerator;
use std::collections::HashMap;

pub use enumerator::S3SplitEnumerator;
mod source;
use serde::Deserialize;
pub use source::S3FileReader;

pub const S3_CONNECTOR: &str = "s3";

#[derive(Clone, Debug, Deserialize)]
pub struct S3Properties {
    #[serde(rename = "s3.region_name")]
    pub region_name: String,
    #[serde(rename = "s3.bucket_name")]
    pub bucket_name: String,
    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,
    #[serde(rename = "s3.credentials.access", default)]
    pub access: Option<String>,
    #[serde(rename = "s3.credentials.secret", default)]
    pub secret: Option<String>,
}

impl From<S3Properties> for HashMap<String, String> {
    fn from(props: S3Properties) -> Self {
        let mut m = HashMap::with_capacity(5);
        m.insert("region".to_owned(), props.region_name);

        if props.access.is_some() {
            m.insert("access_key".to_owned(), props.access.unwrap());
        }
        if props.secret.is_some() {
            m.insert("secret_access".to_owned(), props.secret.unwrap());
        }
        m
    }
}
