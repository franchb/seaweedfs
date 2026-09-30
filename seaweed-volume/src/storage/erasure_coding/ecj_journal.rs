//! Process-wide coordination for `.ecj` deletion journals, and the set-union
//! merge used when a peer's journal is copied in. Mirrors
//! `weed/storage/erasure_coding/ecj_journal.go`.
//!
//! Several `EcVolume`s can hold the same journal: with `-dir.idx` every disk
//! resolves it to one path, and cross-disk reconcile (#9212) mounts a disk's
//! orphan shards against a sibling disk's index. Mount-time compaction
//! replaces the journal's inode, so it may only run while nothing else has the
//! old inode open for append. Each journal path gets one lock, held across an
//! `EcVolume`'s open + load + compact and across every [`merge_ecj_file`], and
//! a count of registered holders read under it. A holder registered under the
//! lock is visible to every later compaction, and a merge never straddles one.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use crate::storage::types::{NEEDLE_ID_SIZE, NeedleId};

/// Suffix of the temp file compaction writes before renaming it over the
/// journal. A leftover one is only ever an unpublished rewrite: the journal it
/// would have replaced is still whole.
pub const ECJ_COMPACT_TMP_SUFFIX: &str = ".ecj.compact.tmp";

/// Suffix of the staging file a peer's journal is copied into before
/// [`merge_ecj_file`] folds it into the local one.
pub const ECJ_INCOMING_SUFFIX: &str = ".ecj.incoming";

const SCAN_BUF_BYTES: usize = 1 << 20;

/// One journal path: its lock, guarding the number of registered holders.
#[derive(Default)]
struct Journal {
    holders: Mutex<usize>,
}

static JOURNALS: LazyLock<Mutex<HashMap<PathBuf, Arc<Journal>>>> = LazyLock::new(Default::default);

fn journal_key(path: &str) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| PathBuf::from(path))
}

/// Run `f` with `path`'s journal lock held. `f` receives the number of
/// `EcVolume`s registered as holders of the journal and may change it.
///
/// Lock order is registry map, then journal; `f` must not re-enter this
/// function (so it must not drop an [`EcjHolder`]).
pub(crate) fn with_ecj_journal<R>(path: &str, f: impl FnOnce(&mut usize) -> R) -> R {
    let key = journal_key(path);
    let journal = JOURNALS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(key.clone())
        .or_default()
        .clone();
    let result = f(&mut journal
        .holders
        .lock()
        .unwrap_or_else(PoisonError::into_inner));

    // Drop the entry once nothing holds it or is waiting on it. Every clone of
    // the Arc is taken under the map lock, so with it held a strong count of
    // two (the map's and ours) means no one else can be inside `f`.
    let mut map = JOURNALS.lock().unwrap_or_else(PoisonError::into_inner);
    if Arc::strong_count(&journal) == 2
        && *journal
            .holders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            == 0
    {
        map.remove(&key);
    }
    result
}

/// An `EcVolume`'s registration as a holder of its journal. Created after the
/// count was raised under the journal lock; dropping it lowers the count.
pub(crate) struct EcjHolder {
    path: String,
}

impl EcjHolder {
    /// Must be paired with a `*holders += 1` made inside [`with_ecj_journal`]
    /// for the same path.
    pub(crate) fn registered(path: &str) -> Self {
        EcjHolder {
            path: path.to_string(),
        }
    }
}

impl Drop for EcjHolder {
    fn drop(&mut self) {
        with_ecj_journal(&self.path, |holders| *holders = holders.saturating_sub(1));
    }
}

#[cfg(test)]
pub(crate) fn holders_of(path: &str) -> usize {
    let key = journal_key(path);
    let journal = JOURNALS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
        .cloned();
    journal.map_or(0, |j| {
        *j.holders.lock().unwrap_or_else(PoisonError::into_inner)
    })
}

/// Options for creating a journal-family file with the same 0644 mode the Go
/// server uses, rather than 0666 minus whatever the umask allows.
pub(crate) fn create_mode_0644(opts: &mut OpenOptions) -> &mut OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o644);
    }
    opts
}

/// Fold the journal at `src_path` into `dst_path` as a set union: only ids
/// that `dst_path` does not already hold are appended, once each. Returns how
/// many were added.
///
/// The journal is a set of deleted needle ids. Appending a peer's whole
/// journal onto the local one (what shard copy and index recovery used to do)
/// is correct but never dedupes, so a volume whose shards move back and forth
/// grows its journal geometrically. A union keeps it at one entry per id.
///
/// A missing `src_path` is a no-op. A torn tail is ignored on the source and
/// truncated on the destination, where a partial record would misalign the
/// append. Runs under the journal lock, so it never interleaves with a
/// mount's load and compaction of the same file.
pub fn merge_ecj_file(dst_path: &str, src_path: &str) -> io::Result<usize> {
    let incoming = match read_ecj_ids(src_path) {
        Ok(ids) => ids,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    with_ecj_journal(dst_path, |_| merge_locked(dst_path, &incoming))
}

fn merge_locked(dst_path: &str, incoming: &[NeedleId]) -> io::Result<usize> {
    let mut dst =
        create_mode_0644(OpenOptions::new().read(true).write(true).create(true)).open(dst_path)?;
    let mut size = dst.metadata()?.len();
    let ragged = size % NEEDLE_ID_SIZE as u64;
    if ragged != 0 {
        size -= ragged;
        dst.set_len(size)?;
    }

    let mut have: HashSet<NeedleId> = HashSet::new();
    scan_ecj((&dst).take(size), |id| {
        have.insert(id);
    })?;
    let mut out = Vec::with_capacity(incoming.len() * NEEDLE_ID_SIZE);
    let mut rec = [0u8; NEEDLE_ID_SIZE];
    for &id in incoming {
        if have.insert(id) {
            id.to_bytes(&mut rec);
            out.extend_from_slice(&rec);
        }
    }
    if out.is_empty() {
        return Ok(0);
    }

    let appended = dst
        .seek(SeekFrom::Start(size))
        .and_then(|_| dst.write_all(&out))
        .and_then(|_| dst.sync_all());
    if let Err(e) = appended {
        let _ = dst.set_len(size);
        return Err(e);
    }
    Ok(out.len() / NEEDLE_ID_SIZE)
}

/// Every whole record of a journal, in file order.
pub(crate) fn read_ecj_ids(path: &str) -> io::Result<Vec<NeedleId>> {
    let mut ids = Vec::new();
    scan_ecj(File::open(path)?, |id| ids.push(id))?;
    Ok(ids)
}

/// Call `f` for each whole record in `r`, ignoring a torn tail.
fn scan_ecj(r: impl Read, mut f: impl FnMut(NeedleId)) -> io::Result<()> {
    let mut r = BufReader::with_capacity(SCAN_BUF_BYTES, r);
    let mut rec = [0u8; NEEDLE_ID_SIZE];
    loop {
        match r.read_exact(&mut rec) {
            Ok(()) => f(NeedleId::from_bytes(&rec)),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_ids(path: &str, ids: &[u64], tail: &[u8]) {
        let mut bytes = Vec::new();
        for &id in ids {
            let mut rec = [0u8; NEEDLE_ID_SIZE];
            NeedleId(id).to_bytes(&mut rec);
            bytes.extend_from_slice(&rec);
        }
        bytes.extend_from_slice(tail);
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn merge_is_a_set_union_and_repairs_a_torn_destination() {
        let tmp = TempDir::new().unwrap();
        let dst = tmp.path().join("7.ecj").to_str().unwrap().to_string();
        let src = tmp
            .path()
            .join("7.ecj.incoming")
            .to_str()
            .unwrap()
            .to_string();
        write_ids(&dst, &[1, 2, 3, 1, 2, 3], &[0xAB, 0xCD]);
        write_ids(&src, &[3, 4, 5, 4, 3, 4, 5, 4], &[]);

        assert_eq!(merge_ecj_file(&dst, &src).unwrap(), 2);
        let got: Vec<u64> = read_ecj_ids(&dst).unwrap().iter().map(|id| id.0).collect();
        assert_eq!(got, vec![1, 2, 3, 1, 2, 3, 4, 5]);

        // Merging the same journal again adds nothing.
        assert_eq!(merge_ecj_file(&dst, &src).unwrap(), 0);
        assert_eq!(
            std::fs::metadata(&dst).unwrap().len(),
            8 * NEEDLE_ID_SIZE as u64
        );
        assert_eq!(holders_of(&dst), 0);
    }

    #[test]
    fn merge_from_a_missing_source_is_a_noop() {
        let tmp = TempDir::new().unwrap();
        let dst = tmp.path().join("7.ecj").to_str().unwrap().to_string();
        let src = tmp.path().join("missing").to_str().unwrap().to_string();
        assert_eq!(merge_ecj_file(&dst, &src).unwrap(), 0);
        assert!(!std::path::Path::new(&dst).exists());
    }

    #[test]
    fn holder_registration_counts_and_releases() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("9.ecj").to_str().unwrap().to_string();
        let register = || {
            with_ecj_journal(&path, |h| *h += 1);
            EcjHolder::registered(&path)
        };
        let a = register();
        let b = register();
        assert_eq!(holders_of(&path), 2);
        drop(a);
        assert_eq!(holders_of(&path), 1);
        drop(b);
        assert_eq!(holders_of(&path), 0);
    }
}
