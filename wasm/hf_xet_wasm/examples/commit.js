/**
 * Commits a file to a Hugging Face dataset.
 *
 * @param {string} hf_endpoint - The HF Hub endpoint.
 * @param {string} file_name The name of the file to commit.
 * @param {string} sha256 The SHA256 hash of the file (as a string).
 * @param {number} file_size The size of the file in bytes.
 * @param {string} repo_type The type of the repo, i.e. "dataset" or "model" or "space"
 * @param {string} repo_id The id of the repo, specified as a namespace and a repo name separated by a '/'
 * @param {string} revision The revision to make the commit on top of
 * @param {string} hf_token The HF token for auth
 * @returns {Promise<string>} A promise that resolves if the commit is successful, or rejects if an error occurs.
 */
async function commit(hf_endpoint, file_name, sha256, file_size, repo_type, repo_id, revision, hf_token) {
    const obj1 = {
        key: "header",
        value: {
            summary: `Upload ${file_name} with hf_xet_wasm`,
            description: ""
        }
    };

    const obj2 = {
        key: "lfsFile",
        value: {
            path: file_name,
            algo: "sha256",
            oid: sha256,
            size: file_size
        }
    };

    // Serialize to JSON string and concatenate with a newline.
    const body = `${JSON.stringify(obj1)}\n${JSON.stringify(obj2)}`;

    const url = `${hf_endpoint}/api/${repo_type}s/${repo_id}/commit/${revision}`;

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${hf_token}`,
                'Content-Type': 'application/x-ndjson'
            },
            body: body
        });

        // Check for HTTP errors.
        // `response.ok` is true for 2xx status codes.
        // This is the equivalent of `error_for_status()`.
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`HTTP error! Status: ${response.status}, Body: ${errorText}`);
        }

        // Get the response text.
        const responseText = await response.text();

        return responseText;
    } catch (error) {
        // Handle any network errors or errors thrown by the fetch API.
        // This is the equivalent of `anyhow::Result<()>`.
        console.error("Commit failed:", error);
        throw error; // Re-throw the error so the caller can handle it
    }
}