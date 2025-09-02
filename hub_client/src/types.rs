use serde::Deserialize;

/// This defines the response format from the Huggingface Hub Xet CAS access token API.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CasJWTInfo {
    pub cas_url: String, // CAS server endpoint base URL
    pub exp: u64,        // access token expiry since UNIX_EPOCH
    pub access_token: String,
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::types::CasJWTInfo;

    #[test]
    fn test_cas_jwt_response_deser() -> Result<()> {
        let bytes = r#"{"casUrl":"https://cas-server.xethub.hf.co","exp":1756489133,"accessToken":"ey...jQ"}"#;

        let info: CasJWTInfo = serde_json::from_slice(bytes.as_bytes())?;

        assert_eq!(info.cas_url, "https://cas-server.xethub.hf.co");
        assert_eq!(info.exp, 1756489133);
        assert_eq!(info.access_token, "ey...jQ");

        Ok(())
    }
}
