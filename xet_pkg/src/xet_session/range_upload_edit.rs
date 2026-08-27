//! XetRangeUploadEdit — pending data for a single edit within a range upload.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use xet_data::processing::DirtyInput;

#[derive(Debug)]
pub enum RangeUploadEditError {
    AlreadyFinished,
}

impl std::fmt::Display for RangeUploadEditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RangeUploadEditError::AlreadyFinished => write!(f, "edit already finished"),
        }
    }
}

impl std::error::Error for RangeUploadEditError {}

// ── XetRangeUploadEditInner ─────────────────────────────────────────────────

pub(super) struct XetRangeUploadEditInner {
    pub(super) original_range: Range<u64>,
    pub(super) new_length: u64,
    /// The accumulated data for this edit. None means it has been consumed.
    pub(super) data: Mutex<Option<Vec<u8>>>,
}

impl XetRangeUploadEditInner {
    fn write(&self, data: &[u8]) {
        let mut guard = self.data.lock().unwrap();
        if let Some(buf) = guard.as_mut() {
            buf.extend_from_slice(data);
        }
    }

    /// Finalise the edit, returning the pending [`DirtyInput`] and clearing the buffer.
    fn finish(self: &Arc<Self>) -> Result<DirtyInput, RangeUploadEditError> {
        let mut guard = self.data.lock().unwrap();
        let data = guard.take().ok_or(RangeUploadEditError::AlreadyFinished)?;
        Ok(DirtyInput {
            original_range: self.original_range.clone(),
            new_length: self.new_length,
            reader: Box::pin(std::io::Cursor::new(data.to_vec())),
        })
    }

    /// Returns the pending data without blocking, or `None` if already finished.
    fn try_finish(self: &Arc<Self>) -> Option<DirtyInput> {
        let mut guard = self.data.lock().ok()?;
        let data = guard.take()?;
        Some(DirtyInput {
            original_range: self.original_range.clone(),
            new_length: self.new_length,
            reader: Box::pin(std::io::Cursor::new(data.to_vec())),
        })
    }
}

// ── XetRangeUploadEdit (public wrapper) ──────────────────────────────────────

/// Handle for a single edit within a [`XetRangeUploadCommit`].
///
/// Returned by [`XetRangeUploadCommit::edit`], [`insert`], and [`delete`].
/// Feed data incrementally with [`write`], then call [`finish`] to obtain the
/// pending [`DirtyInput`].
///
/// **`finish` must be called before [`XetRangeUploadCommit::commit`]**.
///
/// [`write`]: Self::write
/// [`finish`]: Self::finish
/// [`insert`]: XetRangeUploadCommit::insert
/// [`delete`]: XetRangeUploadCommit::delete
#[derive(Clone)]
pub struct XetRangeUploadEdit {
    pub(super) inner: Arc<XetRangeUploadEditInner>,
}

impl std::fmt::Debug for XetRangeUploadEdit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XetRangeUploadEdit")
            .field("original_range", &self.inner.original_range)
            .field("new_length", &self.inner.new_length)
            .finish_non_exhaustive()
    }
}

impl XetRangeUploadEdit {
    pub(super) fn new(original_range: Range<u64>, new_length: u64) -> Self {
        Self {
            inner: Arc::new(XetRangeUploadEditInner {
                original_range,
                new_length,
                data: Mutex::new(Some(Vec::new())),
            }),
        }
    }

    /// Feed data into this edit.
    ///
    /// May be called any number of times before [`finish`].
    ///
    /// [`finish`]: Self::finish
    pub fn write(&self, data: &[u8]) {
        self.inner.write(data);
    }

    /// Finalise the edit, returning the pending [`DirtyInput`].
    ///
    /// Must be called before [`XetRangeUploadCommit::commit`].  A second call returns `Err`
    /// after a successful finish; use [`try_finish`] to read cached data without
    /// finalising again.
    ///
    /// [`try_finish`]: Self::try_finish
    pub fn finish(self: &Arc<Self>) -> Result<DirtyInput, RangeUploadEditError> {
        self.inner.finish()
    }

    /// Returns the pending data without blocking, or `None` if already finished.
    pub fn try_finish(self: &Arc<Self>) -> Option<DirtyInput> {
        self.inner.try_finish()
    }
}
