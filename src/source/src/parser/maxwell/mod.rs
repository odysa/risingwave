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

#[cfg(not(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
)))]
mod json_parser;
mod operators;
#[cfg(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
))]
mod simd_json_parser;

#[cfg(not(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
)))]
pub use json_parser::*;
#[cfg(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
))]
pub use simd_json_parser::*;

#[cfg(test)]
mod test {
    use risingwave_common::array::Op;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};
    use risingwave_expr::vector_op::cast::str_to_timestamp;

    use super::*;
    use crate::{SourceColumnDesc, SourceParser, SourceStreamChunkBuilder};
    #[test]
    fn test_json_parser() {
        let parser = MaxwellParser;
        let descs = vec![
            SourceColumnDesc::simple("id", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("name", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("is_adult", DataType::Int16, 2.into()),
            SourceColumnDesc::simple("birthday", DataType::Timestamp, 3.into()),
        ];

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 4);
        let payloads = vec![
            br#"{"database":"test","table":"t","type":"insert","ts":1666937996,"xid":1171,"commit":true,"data":{"id":1,"name":"tom","is_adult":0,"birthday":"2017-12-31 16:00:01"}}"#.as_slice(),
            br#"{"database":"test","table":"t","type":"insert","ts":1666938023,"xid":1254,"commit":true,"data":{"id":2,"name":"alex","is_adult":1,"birthday":"1999-12-31 16:00:01"}}"#.as_slice(),
            br#"{"database":"test","table":"t","type":"update","ts":1666938068,"xid":1373,"commit":true,"data":{"id":2,"name":"chi","is_adult":1,"birthday":"1999-12-31 16:00:01"},"old":{"name":"alex"}}"#.as_slice()
        ];

        for payload in payloads {
            let writer = builder.row_writer();
            parser.parse(payload, writer).unwrap();
        }

        let chunk = builder.finish();

        let mut rows = chunk.rows();

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.value_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
            assert_eq!(
                row.value_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("tom".into())))
            );
            assert_eq!(
                row.value_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(0)))
            );
            assert_eq!(
                row.value_at(3).to_owned_datum(),
                (Some(ScalarImpl::NaiveDateTime(
                    str_to_timestamp("2017-12-31 16:00:01").unwrap()
                )))
            )
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.value_at(0).to_owned_datum(), Some(ScalarImpl::Int32(2)));
            assert_eq!(
                row.value_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("alex".into())))
            );
            assert_eq!(
                row.value_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(1)))
            );
            assert_eq!(
                row.value_at(3).to_owned_datum(),
                (Some(ScalarImpl::NaiveDateTime(
                    str_to_timestamp("1999-12-31 16:00:01").unwrap()
                )))
            )
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::UpdateDelete);
            assert_eq!(row.value_at(0).to_owned_datum(), Some(ScalarImpl::Int32(2)));
            assert_eq!(
                row.value_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("alex".into())))
            );
            assert_eq!(
                row.value_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(1)))
            );
            assert_eq!(
                row.value_at(3).to_owned_datum(),
                (Some(ScalarImpl::NaiveDateTime(
                    str_to_timestamp("1999-12-31 16:00:01").unwrap()
                )))
            )
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::UpdateInsert);
            assert_eq!(row.value_at(0).to_owned_datum(), Some(ScalarImpl::Int32(2)));
            assert_eq!(
                row.value_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("chi".into())))
            );
            assert_eq!(
                row.value_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(1)))
            );
            assert_eq!(
                row.value_at(3).to_owned_datum(),
                (Some(ScalarImpl::NaiveDateTime(
                    str_to_timestamp("1999-12-31 16:00:01").unwrap()
                )))
            )
        }
    }
}
