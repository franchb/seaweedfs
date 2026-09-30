//! Deduplicated `.ecj` merge for EC shard copy / index recovery.
//!
//! An EC volume's deletion journal (`<vid>.ecj`) is a *set* of deleted needle
//! ids (8-byte big-endian records). `VolumeEcShardsCopy` and EC index recovery
//! used to append the peer's whole journal onto the destination's, so every
//! `ec_balance` round trip roughly doubled the file. This module writes the
//! destination as the deduplicated union (`local ∪ source`), sorted ascending
//! for determinism, via temp + fsync + rename + fsync-dir.

use std::collections::HashSet;
use std::fs;
use std::io::{self, Write};

use crate::storage::types::{NEEDLE_ID_SIZE, NeedleId};
use crate::storage::volume::fsync_dir;

/// Read the id set from an `.ecj` file. Missing file => empty set. A torn
/// trailing partial record is ignored (it was never readable).
pub(crate) fn read_ecj_ids(path: &str) -> io::Result<HashSet<NeedleId>> {
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(e) => return Err(e),
    };
    let count = data.len() / NEEDLE_ID_SIZE;
    let mut ids = HashSet::with_capacity(count);
    for chunk in data[..count * NEEDLE_ID_SIZE].chunks_exact(NEEDLE_ID_SIZE) {
        ids.insert(NeedleId::from_bytes(chunk));
    }
    Ok(ids)
}

/// Write `ids` sorted ascending to `tmp_path` (8-byte BE records), fsync it.
fn write_ecj_ids_sorted(tmp_path: &str, ids: &[NeedleId]) -> io::Result<()> {
    let mut file = fs::File::create(tmp_path)?;
    let mut buf = [0u8; NEEDLE_ID_SIZE];
    for id in ids {
        id.to_bytes(&mut buf);
        file.write_all(&buf)?;
    }
    file.sync_all()?;
    drop(file);
    Ok(())
}

/// Merge `local_path ∪ incoming_path` into `dest_path` as a deduplicated,
/// sorted journal via `<dest>.tmp` + rename. Returns the distinct id count.
///
/// All three paths may coincide (`local == dest` is the normal copy case).
/// Missing inputs read as empty. The write is atomic from readers' point of
/// view: failures before rename leave `dest` untouched (caller removes tmps).
pub(crate) fn merge_ecj_union(
    local_path: &str,
    incoming_path: &str,
    dest_path: &str,
) -> io::Result<usize> {
    let mut union = read_ecj_ids(local_path)?;
    if incoming_path != local_path {
        union.extend(read_ecj_ids(incoming_path)?);
    }
    let mut sorted: Vec<NeedleId> = union.into_iter().collect();
    sorted.sort_unstable();

    let tmp_path = format!("{}.tmp", dest_path);
    let write_result = (|| -> io::Result<()> {
        write_ecj_ids_sorted(&tmp_path, &sorted)?;
        #[cfg(windows)]
        {
            let _ = fs::remove_file(dest_path);
        }
        fs::rename(&tmp_path, dest_path)?;
        fsync_dir(dest_path)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result?;
    Ok(sorted.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn write_ids(path: &str, ids: &[u64]) {
        let mut buf = Vec::with_capacity(ids.len() * NEEDLE_ID_SIZE);
        let mut rec = [0u8; NEEDLE_ID_SIZE];
        for id in ids {
            NeedleId(*id).to_bytes(&mut rec);
            buf.extend_from_slice(&rec);
        }
        fs::write(path, buf).expect("seed ecj");
    }

    fn read_ids_sorted(path: &str) -> Vec<u64> {
        let data = fs::read(path).expect("read ecj");
        assert_eq!(data.len() % NEEDLE_ID_SIZE, 0, "journal must stay aligned");
        let mut ids: Vec<u64> = data
            .chunks_exact(NEEDLE_ID_SIZE)
            .map(|c| NeedleId::from_bytes(c).0)
            .collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn merges_union_deduplicated_and_sorted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let local = dir.path().join("vol.ecj");
        let incoming = dir.path().join("vol.ecj.incoming");
        let dest = dir.path().join("vol.ecj");
        // Distinct files for local/dest here to exercise the 3-path form;
        // the copy path uses local == dest.
        write_ids(local.to_str().unwrap(), &[1, 2, 3]);
        write_ids(incoming.to_str().unwrap(), &[3, 4]);
        // Seed dest with the old local contents (copy path has them already).
        write_ids(dest.to_str().unwrap(), &[1, 2, 3]);

        let n = merge_ecj_union(
            local.to_str().unwrap(),
            incoming.to_str().unwrap(),
            dest.to_str().unwrap(),
        )
        .expect("merge");
        assert_eq!(n, 4);
        assert_eq!(read_ids_sorted(dest.to_str().unwrap()), vec![1, 2, 3, 4]);
        let meta = fs::metadata(dest.to_str().unwrap()).expect("stat");
        assert_eq!(meta.len(), 4 * NEEDLE_ID_SIZE as u64);
    }

    #[test]
    fn missing_inputs_read_as_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("vol.ecj");
        let n = merge_ecj_union(
            dir.path().join("no-local.ecj").to_str().unwrap(),
            dir.path().join("no-incoming.ecj").to_str().unwrap(),
            dest.to_str().unwrap(),
        )
        .expect("merge");
        assert_eq!(n, 0);
        assert_eq!(fs::metadata(dest.to_str().unwrap()).expect("stat").len(), 0);
    }

    #[test]
    fn repeated_merge_stays_constant_size() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("vol.ecj");
        let incoming = dir.path().join("vol.ecj.incoming");
        write_ids(dest.to_str().unwrap(), &[1, 2, 3, 4]);
        write_ids(incoming.to_str().unwrap(), &[3, 4]);
        // A->B->A->B 20 times: the old append path grew geometrically.
        for _ in 0..20 {
            let n = merge_ecj_union(
                dest.to_str().unwrap(),
                incoming.to_str().unwrap(),
                dest.to_str().unwrap(),
            )
            .expect("merge");
            assert_eq!(n, 4);
        }
        assert_eq!(read_ids_sorted(dest.to_str().unwrap()), vec![1, 2, 3, 4]);
    }

    #[test]
    fn torn_tail_is_ignored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let local = dir.path().join("vol.ecj");
        let incoming = dir.path().join("vol.ecj.incoming");
        let dest = dir.path().join("vol.ecj");
        write_ids(local.to_str().unwrap(), &[1, 2]);
        // Append a torn 3-byte tail to the incoming journal.
        write_ids(incoming.to_str().unwrap(), &[2, 3]);
        {
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(incoming.to_str().unwrap())
                .expect("open");
            f.write_all(&[9, 9, 9]).expect("torn tail");
        }
        let ids = read_ecj_ids(incoming.to_str().unwrap()).expect("read");
        let want: HashSet<NeedleId> =
            [NeedleId(2), NeedleId(3)].into_iter().collect();
        assert_eq!(ids, want);
        let n = merge_ecj_union(
            local.to_str().unwrap(),
            incoming.to_str().unwrap(),
            dest.to_str().unwrap(),
        )
        .expect("merge");
        assert_eq!(n, 3);
    }
}
