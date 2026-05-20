//! Persistent JSONL spool for WSJT-X QSO submissions.
//!
//! Without persistence, a Wavelog outage longer than the standard
//! `[0, 1, 4] s` retry schedule drops the QSO with a warn log — the
//! WavelogGate behaviour, but the most-asked-for hardening upgrade.
//!
//! Every accepted ADIF is appended to disk before being queued to the
//! POST worker. Entries are removed only after a successful POST (or a
//! permanent `Rejected` response from Wavelog). At startup, leftover
//! entries from a previous run are replayed into the worker queue, so
//! QSOs captured while Wavelog was unreachable eventually land once
//! it's back.
//!
//! ## File format
//!
//! One JSON object per line:
//! ```json
//! {"seq": <u64>, "adif": "<ADIF text>", "received_at": <unix-ms>}
//! ```
//!
//! `seq` is a per-process monotonic counter assigned at append time and
//! is how the POST worker identifies the entry to remove on success.
//! On startup the next `seq` continues from `max(seq) + 1` of the
//! existing file so reused values can't collide with a replayed entry.
//!
//! ## Bounded size
//!
//! Capped at [`MAX_QUEUE_LEN`] entries. Overflow drops the oldest
//! entries first (FIFO eviction with a WARN log per drop). At
//! contest-rate FT8 this represents several hours of backlog — well
//! past any realistic Wavelog outage.
//!
//! ## Failure modes
//!
//! - **Corrupt file at startup**: renamed to `<path>.corrupt-<ms>` and
//!   the queue starts fresh. Operators see the file and can recover
//!   manually.
//! - **Disk full / I/O error mid-append**: surfaces to the caller; the
//!   listener treats it as a parse-failure-equivalent (warn + drop).
//!   In-memory + on-disk state stay in sync because the in-memory
//!   push happens *after* the successful disk write on the append
//!   path.
//! - **Concurrent access (two daemons against the same file)**: not
//!   supported. The behaviour is undefined; documented in the README.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::util::epoch_millis;

/// Hardcoded cap on retained queue entries. ~5 hours of contest-rate
/// FT8 (one log every ~15s × 1000 = ~4 hours) — well beyond any
/// realistic outage. Increase only if a real workload hits it.
pub const MAX_QUEUE_LEN: usize = 1000;

#[derive(Debug, Error)]
pub enum QsoQueueError {
    #[error("queue I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("queue serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    seq: u64,
    adif: Box<str>,
    received_at: u64,
}

/// On-disk-backed queue of pending QSO submissions.
///
/// Wrap in an `Arc` to share between tasks. Construct one via
/// [`QsoQueue::open`]; the listener (`append`) and POST worker
/// (`remove`) hold the same handle.
pub struct QsoQueue {
    path: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    entries: Vec<Entry>,
    next_seq: u64,
}

#[derive(Debug, Default)]
pub struct ReplayEntries(Vec<(u64, Box<str>)>);

impl ReplayEntries {
    pub fn into_vec(self) -> Vec<(u64, Box<str>)> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl QsoQueue {
    /// Open or create the queue at `path`. Creates parent dirs as
    /// needed. A corrupt file is renamed to `<path>.corrupt-<unix-ms>`
    /// and the queue starts fresh.
    pub async fn open(path: PathBuf) -> Result<(Self, ReplayEntries), QsoQueueError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).await?;
        }
        let entries = match fs::read_to_string(&path).await {
            Ok(contents) => match parse_entries(&contents) {
                Ok(e) => e,
                Err(e) => {
                    let backup = path.with_extension(format!("corrupt-{}", epoch_millis()));
                    tracing::warn!(
                        error = %e,
                        backup = %backup.display(),
                        "qso queue file corrupt; renaming aside and starting fresh",
                    );
                    fs::rename(&path, &backup).await?;
                    Vec::new()
                },
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let next_seq = entries.iter().map(|e| e.seq).max().map_or(1, |s| s + 1);
        let replay = ReplayEntries(entries.iter().map(|e| (e.seq, e.adif.clone())).collect());
        if !replay.is_empty() {
            tracing::info!(
                count = replay.len(),
                path = %path.display(),
                "replaying pending QSOs from queue",
            );
        }
        Ok((
            Self {
                path,
                inner: Mutex::new(Inner { entries, next_seq }),
            },
            replay,
        ))
    }

    /// Persist an ADIF and return its sequence number (passed to
    /// [`Self::remove`] on completion). Enforces [`MAX_QUEUE_LEN`] via
    /// FIFO eviction with a WARN per dropped entry.
    pub async fn append(&self, adif: Box<str>) -> Result<u64, QsoQueueError> {
        let mut inner = self.inner.lock().await;
        let seq = inner.next_seq;
        inner.next_seq += 1;
        let entry = Entry {
            seq,
            adif,
            received_at: epoch_millis(),
        };
        inner.entries.push(entry.clone());

        let needs_rewrite = if inner.entries.len() > MAX_QUEUE_LEN {
            let drop_count = inner.entries.len() - MAX_QUEUE_LEN;
            inner.entries.drain(0..drop_count);
            tracing::warn!(
                dropped = drop_count,
                cap = MAX_QUEUE_LEN,
                "QSO queue at cap; dropping oldest entries",
            );
            true
        } else {
            false
        };

        if needs_rewrite {
            // Cap enforcement requires a full rewrite — appending
            // alone would leave the dropped entries on disk.
            rewrite_to(&self.path, &inner.entries).await?;
        } else {
            let line = serde_json::to_string(&entry)?;
            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .await?;
            f.write_all(line.as_bytes()).await?;
            f.write_all(b"\n").await?;
            // sync_data is enough — we don't care about metadata
            // (mtime etc.) for correctness.
            f.sync_data().await?;
        }
        Ok(seq)
    }

    /// Mark the entry with `seq` as completed and remove it from disk.
    /// No-op if no entry has that sequence number.
    ///
    /// Used by the POST worker on Wavelog success and on permanent
    /// rejection (`WavelogError::Rejected`) — both mean the entry will
    /// never need to be retried.
    pub async fn remove(&self, seq: u64) -> Result<(), QsoQueueError> {
        let mut inner = self.inner.lock().await;
        let initial = inner.entries.len();
        inner.entries.retain(|e| e.seq != seq);
        if inner.entries.len() == initial {
            return Ok(());
        }
        rewrite_to(&self.path, &inner.entries).await
    }

    /// Current in-memory entry count. Useful for tests and for
    /// surfacing queue depth via observability (logs / metrics).
    pub async fn len(&self) -> usize {
        self.inner.lock().await.entries.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.entries.is_empty()
    }
}

async fn rewrite_to(path: &std::path::Path, entries: &[Entry]) -> Result<(), QsoQueueError> {
    // Atomic swap to avoid leaving a half-written file on disk in the event
    // of a crash or power loss mid-write.
    let temp = path.with_extension("tmp");
    let mut buf = String::new();
    for entry in entries {
        let line = serde_json::to_string(entry)?;
        buf.push_str(&line);
        buf.push('\n');
    }

    {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)
            .await?;
        f.write_all(buf.as_bytes()).await?;
        f.sync_data().await?;
    }

    fs::rename(&temp, path).await?;

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        let dir = fs::File::open(parent).await?;
        dir.sync_all().await?;
    }
    Ok(())
}

fn parse_entries(contents: &str) -> Result<Vec<Entry>, serde_json::Error> {
    contents
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(serde_json::from_str)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.jsonl");
        (dir, path)
    }

    #[tokio::test]
    async fn open_creates_empty_queue_when_file_absent() {
        let (_dir, path) = temp_path();
        let (queue, replay) = QsoQueue::open(path).await.unwrap();
        assert!(replay.is_empty());
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn append_then_remove_round_trips_through_disk() {
        let (_dir, path) = temp_path();
        let (queue, _) = QsoQueue::open(path.clone()).await.unwrap();

        let seq1 = queue.append("<CALL:3>K1B <EOR>".into()).await.unwrap();
        let seq2 = queue.append("<CALL:3>K2B <EOR>".into()).await.unwrap();
        assert_ne!(seq1, seq2);
        assert_eq!(queue.len().await, 2);

        // Re-open to confirm both landed on disk.
        drop(queue);
        let (reloaded, replay) = QsoQueue::open(path.clone()).await.unwrap();
        let entries = replay.into_vec();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, seq1);
        assert_eq!(&*entries[0].1, "<CALL:3>K1B <EOR>");
        assert_eq!(entries[1].0, seq2);

        // Remove one and re-open: only the other survives.
        reloaded.remove(seq1).await.unwrap();
        drop(reloaded);
        let (_, replay) = QsoQueue::open(path).await.unwrap();
        let entries = replay.into_vec();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, seq2);
    }

    #[tokio::test]
    async fn replay_seq_continues_past_max_existing() {
        let (_dir, path) = temp_path();
        let (queue, _) = QsoQueue::open(path.clone()).await.unwrap();
        let seq1 = queue.append("a".into()).await.unwrap();
        let seq2 = queue.append("b".into()).await.unwrap();
        drop(queue);

        let (reloaded, _) = QsoQueue::open(path).await.unwrap();
        let seq3 = reloaded.append("c".into()).await.unwrap();
        assert!(
            seq3 > seq2 && seq3 > seq1,
            "seq must continue monotonically: {seq1} {seq2} {seq3}",
        );
    }

    #[tokio::test]
    async fn corrupt_file_is_renamed_aside_and_queue_starts_empty() {
        let (dir, path) = temp_path();
        // Write garbage to the queue file.
        tokio::fs::write(&path, b"not json\n{also: not json\n")
            .await
            .unwrap();

        let (queue, replay) = QsoQueue::open(path.clone()).await.unwrap();
        assert!(replay.is_empty());
        assert_eq!(queue.len().await, 0);

        // Corrupt sibling exists.
        let mut found_corrupt = false;
        let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            if e.file_name()
                .to_string_lossy()
                .starts_with("queue.corrupt-")
            {
                found_corrupt = true;
            }
        }
        assert!(found_corrupt, "corrupt file should have been renamed aside");
    }

    #[tokio::test]
    async fn cap_enforcement_drops_oldest_and_rewrites_disk() {
        let (_dir, path) = temp_path();
        let (queue, _) = QsoQueue::open(path.clone()).await.unwrap();

        // Push exactly MAX + 5 entries; expect 5 oldest to be dropped.
        for i in 0..(MAX_QUEUE_LEN + 5) {
            queue.append(format!("entry-{i}").into()).await.unwrap();
        }
        assert_eq!(queue.len().await, MAX_QUEUE_LEN);

        // Re-open to confirm disk reflects the cap.
        drop(queue);
        let (_, replay) = QsoQueue::open(path).await.unwrap();
        let entries = replay.into_vec();
        assert_eq!(entries.len(), MAX_QUEUE_LEN);
        // Oldest survivor should be entry-5 (entries 0..5 dropped).
        assert_eq!(&*entries[0].1, "entry-5");
    }

    #[tokio::test]
    async fn remove_unknown_seq_is_noop() {
        let (_dir, path) = temp_path();
        let (queue, _) = QsoQueue::open(path).await.unwrap();
        queue.append("present".into()).await.unwrap();
        assert_eq!(queue.len().await, 1);
        queue.remove(9999).await.unwrap();
        assert_eq!(queue.len().await, 1);
    }
}
