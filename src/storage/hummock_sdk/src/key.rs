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

use std::cmp::Ordering;
use std::ops::Bound::*;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::ptr;

use bytes::{Buf, BufMut};
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;

use crate::HummockEpoch;

pub const EPOCH_LEN: usize = std::mem::size_of::<HummockEpoch>();
pub const TABLE_PREFIX_LEN: usize = std::mem::size_of::<u32>();

pub type TableKeyRange = (Bound<TableKey<Vec<u8>>>, Bound<TableKey<Vec<u8>>>);
pub type UserKeyRange = (Bound<UserKey<Vec<u8>>>, Bound<UserKey<Vec<u8>>>);
pub type FullKeyRange = (Bound<FullKey<Vec<u8>>>, Bound<FullKey<Vec<u8>>>);

/// Converts user key to full key by appending `epoch` to the user key.
pub fn key_with_epoch(mut user_key: Vec<u8>, epoch: HummockEpoch) -> Vec<u8> {
    let res = epoch.to_be();
    user_key.reserve(EPOCH_LEN);
    let buf = user_key.chunk_mut();

    // TODO: check whether this hack improves performance
    unsafe {
        ptr::copy_nonoverlapping(
            &res as *const _ as *const u8,
            buf.as_mut_ptr() as *mut _,
            EPOCH_LEN,
        );
        user_key.advance_mut(EPOCH_LEN);
    }

    user_key
}

/// Splits a full key into its user key part and epoch part.
#[inline]
pub fn split_key_epoch(full_key: &[u8]) -> (&[u8], &[u8]) {
    let pos = full_key
        .len()
        .checked_sub(EPOCH_LEN)
        .unwrap_or_else(|| panic!("bad full key format: {:?}", full_key));
    full_key.split_at(pos)
}

/// Extract encoded [`UserKey`] from encoded [`FullKey`] without epoch part
pub fn user_key(full_key: &[u8]) -> &[u8] {
    split_key_epoch(full_key).0
}

/// Extract table key from encoded [`UserKey`] without table id part
pub fn table_key(user_key: &[u8]) -> &[u8] {
    &user_key[TABLE_PREFIX_LEN..]
}

#[inline(always)]
/// Extract encoded [`UserKey`] from encoded [`FullKey`] but allow empty slice
pub fn get_user_key(full_key: &[u8]) -> Vec<u8> {
    if full_key.is_empty() {
        vec![]
    } else {
        user_key(full_key).to_vec()
    }
}

/// Extract table id from encoded [`FullKey`]
#[inline(always)]
pub fn get_table_id(full_key: &[u8]) -> u32 {
    let mut buf = full_key;
    buf.get_u32()
}

// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/// Computes the next key of the given key.
///
/// If the key has no successor key (e.g. the input is "\xff\xff"), the result
/// would be an empty vector.
///
/// # Examples
///
/// ```rust
/// use risingwave_hummock_sdk::key::next_key;
/// assert_eq!(next_key(b"123"), b"124");
/// assert_eq!(next_key(b"12\xff"), b"13");
/// assert_eq!(next_key(b"\xff\xff"), b"");
/// assert_eq!(next_key(b"\xff\xfe"), b"\xff\xff");
/// assert_eq!(next_key(b"T"), b"U");
/// assert_eq!(next_key(b""), b"");
/// ```
pub fn next_key(key: &[u8]) -> Vec<u8> {
    if let Some((s, e)) = next_key_no_alloc(key) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extend_from_slice(s);
        res.push(e);
        res
    } else {
        Vec::new()
    }
}

/// Computes the previous key of the given key.
///
/// If the key has no predecessor key (e.g. the input is "\x00\x00"), the result
/// would be a "\xff\xff" vector.
///
/// # Examples
///
/// ```rust
/// use risingwave_hummock_sdk::key::prev_key;
/// assert_eq!(prev_key(b"123"), b"122");
/// assert_eq!(prev_key(b"12\x00"), b"11\xff");
/// assert_eq!(prev_key(b"\x00\x00"), b"\xff\xff");
/// assert_eq!(prev_key(b"\x00\x01"), b"\x00\x00");
/// assert_eq!(prev_key(b"T"), b"S");
/// assert_eq!(prev_key(b""), b"");
/// ```
pub fn prev_key(key: &[u8]) -> Vec<u8> {
    let pos = key.iter().rposition(|b| *b != 0x00);
    match pos {
        Some(pos) => {
            let mut res = Vec::with_capacity(key.len());
            res.extend_from_slice(&key[0..pos]);
            res.push(key[pos] - 1);
            if pos + 1 < key.len() {
                res.push(b"\xff".to_owned()[0]);
            }
            res
        }
        None => {
            vec![0xff; key.len()]
        }
    }
}

fn next_key_no_alloc(key: &[u8]) -> Option<(&[u8], u8)> {
    let pos = key.iter().rposition(|b| *b != 0xff)?;
    Some((&key[..pos], key[pos] + 1))
}

// End Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

/// compute the next epoch, and don't change the bytes of the u8 slice.
/// # Examples
///
/// ```rust
/// use risingwave_hummock_sdk::key::next_epoch;
/// assert_eq!(next_epoch(b"123"), b"124");
/// assert_eq!(next_epoch(b"\xff\x00\xff"), b"\xff\x01\x00");
/// assert_eq!(next_epoch(b"\xff\xff"), b"\x00\x00");
/// assert_eq!(next_epoch(b"\x00\x00"), b"\x00\x01");
/// assert_eq!(next_epoch(b"S"), b"T");
/// assert_eq!(next_epoch(b""), b"");
/// ```
pub fn next_epoch(epoch: &[u8]) -> Vec<u8> {
    let pos = epoch.iter().rposition(|b| *b != 0xff);
    match pos {
        Some(mut pos) => {
            let mut res = Vec::with_capacity(epoch.len());
            res.extend_from_slice(&epoch[0..pos]);
            res.push(epoch[pos] + 1);
            while pos + 1 < epoch.len() {
                res.push(0x00);
                pos += 1;
            }
            res
        }
        None => {
            vec![0x00; epoch.len()]
        }
    }
}

/// compute the prev epoch, and don't change the bytes of the u8 slice.
/// # Examples
///
/// ```rust
/// use risingwave_hummock_sdk::key::prev_epoch;
/// assert_eq!(prev_epoch(b"124"), b"123");
/// assert_eq!(prev_epoch(b"\xff\x01\x00"), b"\xff\x00\xff");
/// assert_eq!(prev_epoch(b"\x00\x00"), b"\xff\xff");
/// assert_eq!(prev_epoch(b"\x00\x01"), b"\x00\x00");
/// assert_eq!(prev_epoch(b"T"), b"S");
/// assert_eq!(prev_epoch(b""), b"");
/// ```
pub fn prev_epoch(epoch: &[u8]) -> Vec<u8> {
    let pos = epoch.iter().rposition(|b| *b != 0x00);
    match pos {
        Some(mut pos) => {
            let mut res = Vec::with_capacity(epoch.len());
            res.extend_from_slice(&epoch[0..pos]);
            res.push(epoch[pos] - 1);
            while pos + 1 < epoch.len() {
                res.push(0xff);
                pos += 1;
            }
            res
        }
        None => {
            vec![0xff; epoch.len()]
        }
    }
}

/// compute the next full key of the given full key
///
/// if the `user_key` has no successor key, the result will be a empty vec

pub fn next_full_key(full_key: &[u8]) -> Vec<u8> {
    let (user_key, epoch) = split_key_epoch(full_key);
    let prev_epoch = prev_epoch(epoch);
    let mut res = Vec::with_capacity(full_key.len());
    if prev_epoch.cmp(&vec![0xff; prev_epoch.len()]) == Ordering::Equal {
        let next_user_key = next_key(user_key);
        if next_user_key.is_empty() {
            return Vec::new();
        }
        res.extend_from_slice(next_user_key.as_slice());
        res.extend_from_slice(prev_epoch.as_slice());
        res
    } else {
        res.extend_from_slice(user_key);
        res.extend_from_slice(prev_epoch.as_slice());
        res
    }
}

/// compute the prev full key of the given full key
///
/// if the `user_key` has no predecessor key, the result will be a empty vec

pub fn prev_full_key(full_key: &[u8]) -> Vec<u8> {
    let (user_key, epoch) = split_key_epoch(full_key);
    let next_epoch = next_epoch(epoch);
    let mut res = Vec::with_capacity(full_key.len());
    if next_epoch.cmp(&vec![0x00; next_epoch.len()]) == Ordering::Equal {
        let prev_user_key = prev_key(user_key);
        if prev_user_key.cmp(&vec![0xff; prev_user_key.len()]) == Ordering::Equal {
            return Vec::new();
        }
        res.extend_from_slice(prev_user_key.as_slice());
        res.extend_from_slice(next_epoch.as_slice());
        res
    } else {
        res.extend_from_slice(user_key);
        res.extend_from_slice(next_epoch.as_slice());
        res
    }
}

/// Get the end bound of the given `prefix` when transforming it to a key range.
pub fn end_bound_of_prefix(prefix: &[u8]) -> Bound<Vec<u8>> {
    if let Some((s, e)) = next_key_no_alloc(prefix) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extend_from_slice(s);
        res.push(e);
        Excluded(res)
    } else {
        Unbounded
    }
}

/// Get the start bound of the given `prefix` when it is excluded from the range.
pub fn start_bound_of_excluded_prefix(prefix: &[u8]) -> Bound<Vec<u8>> {
    if let Some((s, e)) = next_key_no_alloc(prefix) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extend_from_slice(s);
        res.push(e);
        Included(res)
    } else {
        panic!("the prefix is the maximum value")
    }
}

/// Transform the given `prefix` to a key range.
pub fn range_of_prefix(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    if prefix.is_empty() {
        (Unbounded, Unbounded)
    } else {
        (Included(prefix.to_vec()), end_bound_of_prefix(prefix))
    }
}

/// Prepend the `prefix` to the given key `range`.
pub fn prefixed_range<B: AsRef<[u8]>>(
    range: impl RangeBounds<B>,
    prefix: &[u8],
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = match range.start_bound() {
        Included(b) => Included([prefix, b.as_ref()].concat()),
        Excluded(b) => {
            let b = b.as_ref();
            assert!(!b.is_empty());
            Excluded([prefix, b].concat())
        }
        Unbounded => Included(prefix.to_vec()),
    };

    let end = match range.end_bound() {
        Included(b) => Included([prefix, b.as_ref()].concat()),
        Excluded(b) => {
            let b = b.as_ref();
            assert!(!b.is_empty());
            Excluded([prefix, b].concat())
        }
        Unbounded => end_bound_of_prefix(prefix),
    };

    (start, end)
}

/// [`TableKey`] is an internal concept in storage. It's a wrapper around the key directly from the
/// user, to make the code clearer and avoid confusion with encoded [`UserKey`] and [`FullKey`].
///
/// Its name come from the assumption that Hummock is always accessed by a table-like structure
/// identified by a [`TableId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableKey<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> Deref for TableKey<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: AsRef<[u8]>> DerefMut for TableKey<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for TableKey<T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[inline]
pub fn map_table_key_range(range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> TableKeyRange {
    (range.0.map(TableKey), range.1.map(TableKey))
}

/// [`UserKey`] is is an internal concept in storage. In the storage interface, user specifies
/// `table_key` and `table_id` (in [`ReadOptions`] or [`WriteOptions`]) as the input. The storage
/// will group these two values into one struct for convenient filtering.
///
/// The encoded format is | `table_id` | `table_key` |.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UserKey<T: AsRef<[u8]>> {
    // When comparing `UserKey`, we first compare `table_id`, then `table_key`. So the order of
    // declaration matters.
    pub table_id: TableId,
    pub table_key: TableKey<T>,
}

impl<T: AsRef<[u8]>> UserKey<T> {
    pub fn new(table_id: TableId, table_key: TableKey<T>) -> Self {
        Self {
            table_id,
            table_key,
        }
    }

    /// Pass the inner type of `table_key` to make the code less verbose.
    pub fn for_test(table_id: TableId, table_key: T) -> Self {
        Self {
            table_id,
            table_key: TableKey(table_key),
        }
    }

    /// Encode in to a buffer.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.table_id.table_id());
        buf.put_slice(self.table_key.as_ref());
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + self.table_key.as_ref().len());
        self.encode_into(&mut ret);
        ret
    }

    pub fn is_empty(&self) -> bool {
        self.table_key.as_ref().is_empty()
    }

    /// Get the length of the encoded format.
    pub fn encoded_len(&self) -> usize {
        self.table_key.as_ref().len() + TABLE_PREFIX_LEN
    }
}

impl<'a> UserKey<&'a [u8]> {
    /// Construct a [`UserKey`] from a byte slice. Its `table_key` will be a part of the input
    /// `slice`.
    pub fn decode(slice: &'a [u8]) -> Self {
        let table_id: u32 = (&slice[..]).get_u32();

        Self {
            table_id: TableId::new(table_id),
            table_key: TableKey(&slice[TABLE_PREFIX_LEN..]),
        }
    }

    pub fn to_vec(self) -> UserKey<Vec<u8>> {
        UserKey::new(self.table_id, TableKey(Vec::from(*self.table_key)))
    }
}

impl UserKey<Vec<u8>> {
    pub fn as_ref(&self) -> UserKey<&[u8]> {
        UserKey::new(self.table_id, TableKey(self.table_key.as_slice()))
    }

    /// Use this method to override an old `UserKey<Vec<u8>>` with a `UserKey<&[u8]>` to own the
    /// table key without reallocating a new `UserKey` object.
    pub fn set(&mut self, other: UserKey<&[u8]>) {
        self.table_id = other.table_id;
        self.table_key.clear();
        self.table_key.extend_from_slice(other.table_key.as_ref());
    }
}

impl Default for UserKey<Vec<u8>> {
    fn default() -> Self {
        Self {
            table_id: TableId::default(),
            table_key: TableKey(Vec::new()),
        }
    }
}

/// [`FullKey`] is an internal concept in storage. It associates [`UserKey`] with an epoch.
///
/// The encoded format is | `user_key` | `epoch` |.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FullKey<T: AsRef<[u8]>> {
    pub user_key: UserKey<T>,
    pub epoch: HummockEpoch,
}

impl<T: AsRef<[u8]>> FullKey<T> {
    pub fn new(table_id: TableId, table_key: TableKey<T>, epoch: HummockEpoch) -> Self {
        Self {
            user_key: UserKey::new(table_id, table_key),
            epoch,
        }
    }

    /// Pass the inner type of `table_key` to make the code less verbose.
    pub fn for_test(table_id: TableId, table_key: T, epoch: HummockEpoch) -> Self {
        Self {
            user_key: UserKey::for_test(table_id, table_key),
            epoch,
        }
    }

    /// Encode in to a buffer.
    pub fn encode_into(&self, buf: &mut impl BufMut) {
        self.user_key.encode_into(buf);
        buf.put_u64(self.epoch);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            TABLE_PREFIX_LEN + self.user_key.table_key.as_ref().len() + EPOCH_LEN,
        );
        self.encode_into(&mut buf);
        buf
    }

    pub fn is_empty(&self) -> bool {
        self.user_key.is_empty()
    }

    /// Get the length of the encoded format.
    pub fn encoded_len(&self) -> usize {
        self.user_key.encoded_len() + EPOCH_LEN
    }
}

impl<'a> FullKey<&'a [u8]> {
    /// Construct a [`FullKey`] from a byte slice.
    pub fn decode(slice: &'a [u8]) -> Self {
        let epoch_pos = slice.len() - EPOCH_LEN;
        let epoch = (&slice[epoch_pos..]).get_u64();

        Self {
            user_key: UserKey::decode(&slice[..epoch_pos]),
            epoch,
        }
    }

    pub fn to_vec(self) -> FullKey<Vec<u8>> {
        FullKey {
            user_key: self.user_key.to_vec(),
            epoch: self.epoch,
        }
    }
}

impl FullKey<Vec<u8>> {
    pub fn to_ref(&self) -> FullKey<&[u8]> {
        FullKey {
            user_key: self.user_key.as_ref(),
            epoch: self.epoch,
        }
    }

    /// Use this method to override an old `FullKey<Vec<u8>>` with a `FullKey<&[u8]>` to own the
    /// table key without reallocating a new `FullKey` object.
    pub fn set(&mut self, other: FullKey<&[u8]>) {
        self.user_key.set(other.user_key);
        self.epoch = other.epoch;
    }
}

impl Default for FullKey<Vec<u8>> {
    // Note: Calling `is_empty` on `FullKey::default` will return `true`, so it can be used to
    // represent unbounded range.
    fn default() -> Self {
        Self {
            user_key: UserKey::default(),
            epoch: INVALID_EPOCH,
        }
    }
}

impl<T: AsRef<[u8]> + Ord + Eq> Ord for FullKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // When `user_key` is the same, greater epoch comes first.
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.epoch.cmp(&self.epoch))
    }
}

impl<T: AsRef<[u8]> + Ord + Eq> PartialOrd for FullKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Bound table key range with table id to generate a new user key range.
pub fn bound_table_key_range<T: AsRef<[u8]>>(
    table_id: TableId,
    table_key_range: &impl RangeBounds<TableKey<T>>,
) -> UserKeyRange {
    let start = match table_key_range.start_bound() {
        Included(b) => Included(UserKey::new(table_id, TableKey(b.as_ref().to_vec()))),
        Excluded(b) => Excluded(UserKey::new(table_id, TableKey(b.as_ref().to_vec()))),
        Unbounded => Included(UserKey::new(table_id, TableKey(b"".to_vec()))),
    };

    let end = match table_key_range.end_bound() {
        Included(b) => Included(UserKey::new(table_id, TableKey(b.as_ref().to_vec()))),
        Excluded(b) => Excluded(UserKey::new(table_id, TableKey(b.as_ref().to_vec()))),
        Unbounded => {
            if let Some(next_table_id) = table_id.table_id().checked_add(1) {
                Excluded(UserKey::new(next_table_id.into(), TableKey(b"".to_vec())))
            } else {
                Unbounded
            }
        }
    };

    (start, end)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn test_encode_decode() {
        let table_key = b"abc".to_vec();
        let key = FullKey::for_test(TableId::new(0), &table_key[..], 0);
        let buf = key.encode();
        assert_eq!(FullKey::decode(&buf), key);
        let key = FullKey::for_test(TableId::new(1), &table_key[..], 1);
        let buf = key.encode();
        assert_eq!(FullKey::decode(&buf), key);
    }

    #[test]
    fn test_key_cmp() {
        // 1 compared with 256 under little-endian encoding would return wrong result.
        let key1 = FullKey::for_test(TableId::new(0), b"0".to_vec(), 1);
        let key2 = FullKey::for_test(TableId::new(1), b"0".to_vec(), 1);
        let key3 = FullKey::for_test(TableId::new(1), b"1".to_vec(), 256);
        let key4 = FullKey::for_test(TableId::new(1), b"1".to_vec(), 1);

        assert_eq!(key1.cmp(&key1), Ordering::Equal);
        assert_eq!(key1.cmp(&key2), Ordering::Less);
        assert_eq!(key2.cmp(&key3), Ordering::Less);
        assert_eq!(key3.cmp(&key4), Ordering::Less);
    }

    #[test]
    fn test_prev_key() {
        assert_eq!(prev_key(b"123"), b"122");
        assert_eq!(prev_key(b"12\x00"), b"11\xff");
        assert_eq!(prev_key(b"\x00\x00"), b"\xff\xff");
        assert_eq!(prev_key(b"\x00\x01"), b"\x00\x00");
        assert_eq!(prev_key(b"T"), b"S");
        assert_eq!(prev_key(b""), b"");
    }

    #[test]
    fn test_bound_table_key_range() {
        assert_eq!(
            bound_table_key_range(
                TableId::default(),
                &(
                    Included(TableKey(b"a".to_vec())),
                    Included(TableKey(b"b".to_vec()))
                )
            ),
            (
                Included(UserKey::for_test(TableId::default(), b"a".to_vec())),
                Included(UserKey::for_test(TableId::default(), b"b".to_vec()),)
            )
        );
        assert_eq!(
            bound_table_key_range(
                TableId::from(1),
                &(Included(TableKey(b"a".to_vec())), Unbounded)
            ),
            (
                Included(UserKey::for_test(TableId::from(1), b"a".to_vec())),
                Excluded(UserKey::for_test(TableId::from(2), b"".to_vec()),)
            )
        );
        assert_eq!(
            bound_table_key_range(
                TableId::from(u32::MAX),
                &(Included(TableKey(b"a".to_vec())), Unbounded)
            ),
            (
                Included(UserKey::for_test(TableId::from(u32::MAX), b"a".to_vec())),
                Unbounded,
            )
        );
    }

    #[test]
    fn test_next_full_key() {
        let user_key = b"aaa".to_vec();
        let epoch: HummockEpoch = 3;
        let mut full_key = key_with_epoch(user_key, epoch);
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 2));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 1));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 0));
        full_key = next_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch("aab".as_bytes().to_vec(), HummockEpoch::MAX)
        );
        assert_eq!(
            next_full_key(&key_with_epoch(b"\xff".to_vec(), 0)),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn test_prev_full_key() {
        let user_key = b"aab";
        let epoch: HummockEpoch = HummockEpoch::MAX - 3;
        let mut full_key = key_with_epoch(user_key.to_vec(), epoch);
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX - 2)
        );
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(
            full_key,
            key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX - 1)
        );
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aab".to_vec(), HummockEpoch::MAX));
        full_key = prev_full_key(full_key.as_slice());
        assert_eq!(full_key, key_with_epoch(b"aaa".to_vec(), 0));

        assert_eq!(
            prev_full_key(&key_with_epoch(b"\x00".to_vec(), HummockEpoch::MAX)),
            Vec::<u8>::new()
        );
    }
}
