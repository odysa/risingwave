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

use std::mem::size_of;

use bincode::{config, encode_to_vec};
#[cfg(test)]
use mockall::automock;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::record::Record;
use crate::error::Result;

pub(crate) static MAGIC_BYTES: u32 = 0x484D5452; // HMTR

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub(crate) trait TraceWriter {
    async fn write(&mut self, record: Record) -> Result<usize>;
    async fn flush(&mut self) -> Result<()>;
    async fn write_all(&mut self, records: Vec<Record>) -> Result<usize> {
        let mut total_size = 0;
        for r in records {
            total_size += self.write(r).await?
        }
        Ok(total_size)
    }
}

/// Serializer serializes a record to std write.
#[cfg_attr(test, automock)]
pub(crate) trait Serializer {
    fn serialize(&self, record: Record) -> Result<Vec<u8>>;
}

#[derive(Default)]
pub(crate) struct BincodeSerializer;

impl Serializer for BincodeSerializer {
    fn serialize(&self, record: Record) -> Result<Vec<u8>> {
        let bytes = encode_to_vec(record, config::standard())?;
        Ok(bytes)
    }
}

pub(crate) struct TraceWriterImpl<W: AsyncWrite + Unpin, S: Serializer> {
    writer: W,
    serializer: S,
}

impl<W: AsyncWrite + Unpin, S: Serializer> TraceWriterImpl<W, S> {
    pub(crate) async fn new(mut writer: W, serializer: S) -> Result<Self> {
        assert_eq!(
            writer
                .write(&MAGIC_BYTES.to_be_bytes())
                .await
                .expect("failed to write magic bytes"),
            size_of::<u32>()
        );
        Ok(Self { writer, serializer })
    }
}

impl<W: AsyncWrite + Unpin> TraceWriterImpl<W, BincodeSerializer> {
    pub(crate) async fn new_bincode(writer: W) -> Result<Self> {
        let s = BincodeSerializer::default();
        Self::new(writer, s).await
    }
}

#[async_trait::async_trait]
impl<W, S> TraceWriter for TraceWriterImpl<W, S>
where
    W: AsyncWrite + Unpin + Send,
    S: Serializer + Send,
{
    async fn write(&mut self, record: Record) -> Result<usize> {
        let buf = self.serializer.serialize(record)?;
        let size = self.writer.write(&buf).await?;
        Ok(size)
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use bincode::{config, decode_from_slice, encode_to_vec};
    use bytes::BytesMut;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::{Operation, TracedBytes};

    #[test]
    fn test_bincode_serialize() {
        let op = Operation::get(
            TracedBytes::from(vec![0, 1, 2, 3]),
            123,
            None,
            true,
            Some(12),
            123,
            false,
        );
        let expected = Record::new_local_none(0, op);
        let serializer = BincodeSerializer::default();

        let bytes = serializer.serialize(expected.clone()).unwrap();

        let (actual, read_size) = decode_from_slice(&bytes, config::standard()).unwrap();

        assert_eq!(bytes.len(), read_size);
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_writer_impl_write() {
        let mut temp_file = async_tempfile::TempFile::new().await.unwrap();

        let key = TracedBytes::from(vec![123]);
        let value = TracedBytes::from(vec![234]);
        let op = Operation::ingest(vec![(key, Some(value))], vec![], 0, 0);
        let record = Record::new_local_none(0, op);
        let r_bytes = encode_to_vec(record.clone(), config::standard()).unwrap();
        let r_len = r_bytes.len();

        let mock_serializer = BincodeSerializer::default();

        {
            let mut writer =
                TraceWriterImpl::new(temp_file.open_rw().await.unwrap(), mock_serializer)
                    .await
                    .unwrap();

            writer.write(record).await.unwrap();
        }

        let mut buf = BytesMut::new();
        temp_file.read_buf(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size_of::<u32>() + r_len);
    }
}
