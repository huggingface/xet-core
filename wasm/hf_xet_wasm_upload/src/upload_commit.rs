use wasm_bindgen::prelude::*;
use xet::xet_session::{
    Sha256Policy, XetStreamUpload as InnerStream, XetUploadCommit as InnerCommit,
};

fn js_err(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}

/// Parse a JS-supplied Sha256Policy value.
///
/// Accepts:
///   - `undefined` / `null` -> `Compute`
///   - the string `"compute"` -> `Compute`
///   - the string `"skip"`    -> `Skip`
///   - an object `{ provided: "<64-hex>" }` -> `Provided(...)` (validates length + hex)
fn parse_sha256_policy(value: JsValue) -> Result<Sha256Policy, JsValue> {
    if value.is_undefined() || value.is_null() {
        return Ok(Sha256Policy::Compute);
    }
    if let Some(s) = value.as_string() {
        return match s.as_str() {
            "compute" => Ok(Sha256Policy::Compute),
            "skip" => Ok(Sha256Policy::Skip),
            other => Err(JsValue::from_str(&format!("invalid sha256 policy string: {other:?}"))),
        };
    }
    let provided = js_sys::Reflect::get(&value, &JsValue::from_str("provided")).map_err(js_err)?;
    if let Some(hex) = provided.as_string() {
        if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(JsValue::from_str(&format!(
                "sha256 policy `provided` must be a 64-char hex string, got: {hex:?}",
            )));
        }
        return Ok(Sha256Policy::from_hex(&hex));
    }
    Err(JsValue::from_str(
        "sha256 policy must be \"compute\", \"skip\", or { provided: \"<64-hex>\" }",
    ))
}

#[wasm_bindgen(js_name = "XetUploadCommit")]
pub struct XetUploadCommit {
    inner: InnerCommit,
}

impl XetUploadCommit {
    pub(crate) fn new(inner: InnerCommit) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen(js_class = "XetUploadCommit")]
impl XetUploadCommit {
    /// Upload a complete byte buffer. Returns per-file metadata
    /// (`XetFileMetadata` serialized to a JS object).
    #[wasm_bindgen(js_name = "uploadBytes")]
    pub async fn upload_bytes(
        &self,
        bytes: Vec<u8>,
        sha256_policy: JsValue,
        tracking_name: Option<String>,
    ) -> Result<JsValue, JsValue> {
        let policy = parse_sha256_policy(sha256_policy)?;
        let handle = self
            .inner
            .upload_bytes(bytes, policy, tracking_name)
            .await
            .map_err(js_err)?;
        let meta = handle.finalize_ingestion().await.map_err(js_err)?;
        serde_wasm_bindgen::to_value(&meta).map_err(js_err)
    }

    /// Begin a streaming upload. Returns a handle on which you call
    /// `write(chunk)` repeatedly, then `finish()`.
    #[wasm_bindgen(js_name = "uploadStream")]
    pub async fn upload_stream(
        &self,
        tracking_name: Option<String>,
        sha256_policy: JsValue,
    ) -> Result<XetStreamUpload, JsValue> {
        let policy = parse_sha256_policy(sha256_policy)?;
        let stream = self
            .inner
            .upload_stream(tracking_name, policy)
            .await
            .map_err(js_err)?;
        Ok(XetStreamUpload::new(stream))
    }

    /// Push xorbs + shard to CAS and finalize. Returns the commit report
    /// as a JS object with `dedup_metrics` and `uploads` (keyed by stringified
    /// task id). The internal `progress` snapshot is omitted because
    /// `GroupProgressReport` is not serde-serializable.
    pub async fn commit(&self) -> Result<JsValue, JsValue> {
        let report = self.inner.commit().await.map_err(js_err)?;

        let out = js_sys::Object::new();

        let dedup = serde_wasm_bindgen::to_value(&report.dedup_metrics).map_err(js_err)?;
        js_sys::Reflect::set(&out, &JsValue::from_str("dedup_metrics"), &dedup).map_err(js_err)?;

        let uploads_obj = js_sys::Object::new();
        for (uid, meta) in &report.uploads {
            let key = JsValue::from_str(&uid.0.to_string());
            let val = serde_wasm_bindgen::to_value(meta).map_err(js_err)?;
            js_sys::Reflect::set(&uploads_obj, &key, &val).map_err(js_err)?;
        }
        js_sys::Reflect::set(&out, &JsValue::from_str("uploads"), &uploads_obj).map_err(js_err)?;

        Ok(out.into())
    }

    pub fn abort(&self) -> Result<(), JsValue> {
        self.inner.abort().map_err(js_err)
    }
}

#[wasm_bindgen(js_name = "XetStreamUpload")]
pub struct XetStreamUpload {
    inner: InnerStream,
}

impl XetStreamUpload {
    pub(crate) fn new(inner: InnerStream) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen(js_class = "XetStreamUpload")]
impl XetStreamUpload {
    pub async fn write(&self, chunk: Vec<u8>) -> Result<(), JsValue> {
        self.inner.write(chunk).await.map_err(js_err)
    }

    pub async fn finish(&self) -> Result<JsValue, JsValue> {
        let meta = self.inner.finish().await.map_err(js_err)?;
        serde_wasm_bindgen::to_value(&meta).map_err(js_err)
    }

    pub fn abort(&self) {
        self.inner.abort();
    }
}
