//! Persistent JSONL spool for WSJT-X QSO submissions.
//!
//! Without persistence, a Wavelog outage longer than the standard
//! `[0, 1, 4] s` retry schedule drops the QSO with a warn log.
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
//! entries first (FIFO eviction with a WARN log per drop).
//!
//! ## Failure modes
//!
//! - **Truncated trailing line at startup** (likely artifact of a
//!   crash between `write_all` and `sync_data` on append): dropped
//!   with a WARN. Preceding entries survive and the file is rewritten
//!   to drop the bad line. Mid-file corruption still falls through to
//!   the corrupt-rename path below.
//! - **Corrupt file at startup**: renamed to `<path>.corrupt-<ms>` and
//!   the queue starts fresh. Operators see the file and can recover
//!   manually.
//! - **Disk full or I/O error mid-append**: surfaces to the caller; the
//!   listener treats it as a parse-failure-equivalent (warn + drop).
//!   In-memory + on-disk state stay in sync because the in-memory
//!   push happens after the successful disk write on the append path.
//! - **Concurrent access (two daemons against the same path)**:
//!   prevented by an advisory exclusive lock on `<path>.lock`.
//!   The second daemon errors out with [`QsoQueueError::AlreadyLocked`]
//!   at startup rather than racing writes against the first.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::util::epoch_millis;

/// Hardcoded cap on retained queue entries.
pub const MAX_QUEUE_LEN: usize = 1000;

#[derive(Debug, Error)]
pub enum QsoQueueError {
    #[error("queue I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("queue serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error(
        "QSO queue is already locked by another process (lock file: {}). \
         Is another wavelog-relay daemon already running against the same queue path?",
        _0.display(),
    )]
    AlreadyLocked(Box<Path>),
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
#[derive(Debug)]
pub struct QsoQueue {
    path: PathBuf,
    inner: Mutex<Inner>,
    // Held for the lifetime of QsoQueue. Dropping releases the
    // advisory exclusive lock that prevents a second daemon from
    // mutating the same queue file. Lock is on a sibling `.lock`
    // file (not the queue file itself) so cap-eviction and
    // truncated-trailing rewrites can rename the queue file freely
    // without invalidating the lock.
    _lock: std::fs::File,
}

#[derive(Debug)]
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
    /// needed.
    ///
    /// Acquires an exclusive advisory lock on `<path>.lock`; a second
    /// daemon against the same path fails with
    /// [`QsoQueueError::AlreadyLocked`] rather than racing writes
    /// against the first.
    ///
    /// A truncated trailing line is dropped with a WARN and the file
    /// is rewritten. Any other parse failure is treated as corruption:
    /// the file is renamed to `<path>.corrupt-<unix-ms>` and the queue
    /// starts fresh.
    pub async fn open(path: PathBuf) -> Result<(Self, ReplayEntries), QsoQueueError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).await?;
        }

        let lock = acquire_lock(&path)?;

        let entries = match fs::read_to_string(&path).await {
            Ok(contents) => match parse_entries(&contents) {
                Ok(ParseOutcome {
                    entries,
                    trailing_dropped: false,
                }) => entries,
                Ok(ParseOutcome {
                    entries,
                    trailing_dropped: true,
                }) => {
                    // parse_entries already logged the WARN. Rewrite
                    // so the bad trailing bytes don't survive into
                    // future restarts.
                    rewrite_to(&path, &entries).await?;
                    entries
                },
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
                _lock: lock,
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
            // sync_data is enough, we dont care about
            // metadata for correctness.
            f.sync_data().await?;
        }
        Ok(seq)
    }

    /// Mark the entry with `seq` as completed and remove it from disk.
    /// No-op if no entry has that sequence number.
    ///
    /// Used by the POST worker on Wavelog success and on permanent
    /// rejection (`WavelogError::Rejected`). Both mean the entry will
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

/// Result of parsing the on-disk queue.
///
/// `trailing_dropped` flags the case where the final non-empty line
/// failed to parse but every earlier line was valid. The caller
/// rewrites the file in that case so the partial bytes don't survive
/// into the next restart. Mid-file parse failures (anywhere but the
/// last line) still surface as `Err`.
struct ParseOutcome {
    entries: Vec<Entry>,
    trailing_dropped: bool,
}

fn parse_entries(contents: &str) -> Result<ParseOutcome, serde_json::Error> {
    let non_empty: Vec<&str> = contents.lines().filter(|l| !l.trim().is_empty()).collect();
    let Some(last_idx) = non_empty.len().checked_sub(1) else {
        return Ok(ParseOutcome {
            entries: Vec::new(),
            trailing_dropped: false,
        });
    };
    let mut entries = Vec::with_capacity(non_empty.len());
    for (i, line) in non_empty.iter().enumerate() {
        match serde_json::from_str::<Entry>(line) {
            Ok(e) => entries.push(e),
            Err(e) if i == last_idx => {
                tracing::warn!(
                    error = %e,
                    "qso queue: trailing line malformed (likely truncated mid-write); \
                     dropping last entry and rewriting",
                );
                return Ok(ParseOutcome {
                    entries,
                    trailing_dropped: true,
                });
            },
            Err(e) => return Err(e),
        }
    }
    Ok(ParseOutcome {
        entries,
        trailing_dropped: false,
    })
}

fn acquire_lock(path: &Path) -> Result<std::fs::File, QsoQueueError> {
    let lock_path = path.with_extension("lock");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    match lock.try_lock() {
        Ok(()) => Ok(lock),
        Err(std::fs::TryLockError::WouldBlock) => {
            Err(QsoQueueError::AlreadyLocked(lock_path.into_boxed_path()))
        },
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
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

    #[tokio::test]
    async fn open_drops_truncated_trailing_line_and_keeps_rest() {
        let (_dir, path) = temp_path();
        // Two complete entries followed by a truncated final line, the
        // most likely artifact of a crash between write_all and sync_data
        // on append.
        let good_a = r#"{"seq":1,"adif":"<EOR>","received_at":1}"#;
        let good_b = r#"{"seq":2,"adif":"<EOR>","received_at":2}"#;
        let truncated = r#"{"seq":3,"adif":"<E"#;
        let contents = format!("{good_a}\n{good_b}\n{truncated}");
        tokio::fs::write(&path, contents.as_bytes()).await.unwrap();

        let (queue, replay) = QsoQueue::open(path.clone()).await.unwrap();
        let entries = replay.into_vec();
        assert_eq!(entries.len(), 2, "expected 2 surviving entries");
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[1].0, 2);
        assert_eq!(queue.len().await, 2);

        // Trailing-line truncation must not produce a corrupt-rename
        // sibling; that path is reserved for mid-file corruption.
        let parent = path.parent().unwrap();
        let mut iter = tokio::fs::read_dir(parent).await.unwrap();
        let mut corrupt_found = false;
        while let Some(e) = iter.next_entry().await.unwrap() {
            if e.file_name()
                .to_string_lossy()
                .starts_with("queue.corrupt-")
            {
                corrupt_found = true;
            }
        }
        assert!(
            !corrupt_found,
            "trailing-line truncation must not produce a corrupt-rename",
        );

        // File on disk should have been rewritten to drop the bad line.
        let on_disk = tokio::fs::read_to_string(&path).await.unwrap();
        let line_count = on_disk.lines().filter(|l| !l.trim().is_empty()).count();
        assert_eq!(
            line_count, 2,
            "file should have been rewritten to 2 lines: {on_disk}",
        );
        assert!(
            !on_disk.contains("\"seq\":3"),
            "truncated entry must be gone: {on_disk}",
        );
    }

    #[tokio::test]
    async fn open_treats_mid_file_corruption_as_corrupt() {
        // good, then garbage in the middle, then good. The middle bad
        // line means something stepped on the file out from under us,
        // not just a half-finished append. The whole file is treated
        // as corrupt.
        let (dir, path) = temp_path();
        let contents = format!(
            "{}\n{}\n{}\n",
            r#"{"seq":1,"adif":"<EOR>","received_at":1}"#,
            "not json at all",
            r#"{"seq":2,"adif":"<EOR>","received_at":2}"#,
        );
        tokio::fs::write(&path, contents.as_bytes()).await.unwrap();

        let (queue, replay) = QsoQueue::open(path).await.unwrap();
        assert!(
            replay.is_empty(),
            "mid-file corruption must drop all entries"
        );
        assert_eq!(queue.len().await, 0);

        let mut iter = tokio::fs::read_dir(dir.path()).await.unwrap();
        let mut found = false;
        while let Some(e) = iter.next_entry().await.unwrap() {
            if e.file_name()
                .to_string_lossy()
                .starts_with("queue.corrupt-")
            {
                found = true;
            }
        }
        assert!(
            found,
            "mid-file corruption must produce a corrupt-rename sibling",
        );
    }

    #[tokio::test]
    async fn open_fails_with_already_locked_when_a_second_queue_is_open() {
        let (_dir, path) = temp_path();
        let (_first, _) = QsoQueue::open(path.clone()).await.unwrap();
        let err = QsoQueue::open(path).await.unwrap_err();
        assert!(
            matches!(err, QsoQueueError::AlreadyLocked(_)),
            "second open must fail with AlreadyLocked, got {err:?}",
        );
    }

    #[tokio::test]
    async fn lock_releases_when_queue_is_dropped() {
        let (_dir, path) = temp_path();
        let (first, _) = QsoQueue::open(path.clone()).await.unwrap();
        drop(first);
        let (_second, _) = QsoQueue::open(path)
            .await
            .expect("re-open after drop must succeed");
    }
}
