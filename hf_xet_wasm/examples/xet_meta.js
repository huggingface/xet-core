function xetMetadataOrNone(jsonData) {
	/**
	 * Extract XET metadata from the HTTP body or return null if not found.
	 *
	 * @param {jsonData} - HTTP body in JSON to extract the XET metadata from.
	 * @returns {XetMetadata|null} The extracted metadata or null if missing.
	 */

	const xetEndpoint = jsonData.casUrl;
	const accessToken = jsonData.accessToken;
	const expiration = jsonData.exp;

	if (xetEndpoint == undefined || accessToken == undefined || expiration == undefined) {
		return null;
	}

	const expirationUnixEpoch = parseInt(expiration, 10);
	if (isNaN(expirationUnixEpoch)) {
		return null;
	}

	return {
		endpoint: xetEndpoint,
		accessToken: accessToken,
		expirationUnixEpoch: expirationUnixEpoch,
	};
}

async function fetchXetMetadataFromRepoInfo({
	hfEndpoint,
	tokenType,
	repoId,
	repoType,
	headers,
	params = null
}) {
	/**
	 * Uses the repo info to request a XET access token from Hub.
	 *
	 * @param {string} hfEndpoint - The HF Hub endpoint.
	 * @param {string} tokenType - Type of the token to request: "read" or "write".
	 * @param {string} repoId - A namespace (user or an organization) and a repo name separated by a `/`.
	 * @param {string} repoType - Type of the repo to upload to: "model", "dataset", or "space".
	 * @param {Object} headers - Headers to use for the request, including authorization headers and user agent.
	 * @param {Object|null} params - Additional parameters to pass with the request.
	 * @returns {Promise<Object|null>} The metadata needed to make the request to the XET storage service.
	 * @throws {Error} If the Hub API returned an error or the response is improperly formatted.
	 */

	const url = `${hfEndpoint}/api/${repoType}s/${repoId}/xet-${tokenType}-token/main`;
	console.log(`${url}`);

	return fetchXetMetadataWithUrl(url, headers, params);
}

async function fetchXetMetadataWithUrl(url, headers, params = null) {
	/**
	 * Requests the XET access token from the supplied URL.
	 *
	 * @param {string} url - The access token endpoint URL.
	 * @param {Object} headers - Headers to use for the request, including authorization headers and user agent.
	 * @param {Object|null} params - Additional parameters to pass with the request.
	 * @returns {Promise<Object|null>} The metadata needed to make the request to the XET storage service.
	 * @throws {Error} If the Hub API returned an error or the response is improperly formatted.
	 */

	const response = await fetch(url, {
		method: "GET",
		headers: headers,
	});

	const jsonData = await response.json();

	if (!response.ok) {
		console.log("response not ok");
		throw new Error(`HTTP error! Status: ${response.status}`);
	}

	const metadata = xetMetadataOrNone(jsonData);
	if (!metadata) {
		throw new Error("XET headers have not been correctly set by the server.");
	}

	return metadata;
}