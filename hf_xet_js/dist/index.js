"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.download_xet_files = exports.upload_xet_files = exports.PointerFile = void 0;
const hf_xet_js_1 = require("../pkg/hf_xet_js");
var hf_xet_js_2 = require("../pkg/hf_xet_js");
Object.defineProperty(exports, "PointerFile", { enumerable: true, get: function () { return hf_xet_js_2.PointerFile; } });
const HUGGINGFACE_HEADER_X_XET_ENDPOINT = "X-Xet-Cas-Url";
const HUGGINGFACE_HEADER_X_XET_ACCESS_TOKEN = "X-Xet-Access-Token";
const HUGGINGFACE_HEADER_X_XET_EXPIRATION = "X-Xet-Token-Expiration";
const HUGGINGFACE_HEADER_X_XET_HASH = "X-Xet-Hash";
const HUGGINGFACE_HEADER_X_XET_REFRESH_ROUTE = "X-Xet-Refresh-Route";
class TokenInfo {
    _token;
    _endpoint;
    _expiry;
    constructor(_token, _endpoint, _expiry) {
        this._token = _token;
        this._endpoint = _endpoint;
        this._expiry = _expiry;
    }
    get token() {
        return this._token;
    }
    get endpoint() {
        return this._endpoint;
    }
    get expiry() {
        return this._expiry;
    }
}
class TokenRefresher {
    endpoint;
    headers;
    token_type;
    repo_type;
    repo_id;
    revision;
    constructor(endpoint, repo_type, repo_id, hub_token, token_type, revision) {
        this.endpoint = endpoint;
        this.repo_type = repo_type;
        this.repo_id = repo_id;
        this.headers = { Authorization: `Bearer ${hub_token}` };
        this.token_type = token_type;
        this.revision = revision;
    }
    async refresh_token() {
        const url = `${this.endpoint}/api/${this.repo_type}s/${this.repo_id}/xet-${this.token_type}-token/${this.revision}`;
        let token_response;
        try {
            token_response = await fetch(url, { headers: this.headers });
            console.log(token_response);
        }
        catch (e) {
            console.error(`error refreshing token: ${e}`);
            throw e;
        }
        let response_headers = token_response.headers;
        const cas_endpoint = response_headers.get(HUGGINGFACE_HEADER_X_XET_ENDPOINT) ?? "";
        const token = response_headers.get(HUGGINGFACE_HEADER_X_XET_ACCESS_TOKEN) ?? "";
        const expiry = parseInt(response_headers.get(HUGGINGFACE_HEADER_X_XET_EXPIRATION) ?? "");
        console.log(`expiry ${expiry}`);
        return new TokenInfo(token, cas_endpoint, BigInt(isNaN(expiry) ? 0 : expiry));
    }
}
async function upload_xet_files(repo_type, repo_id, files) {
    const token_refresher = new TokenRefresher("https://huggingface.co", repo_type, repo_id, "hub_token_TODO", "write", "main");
    let token_info = await token_refresher.refresh_token();
    return await (0, hf_xet_js_1.upload_async)(files, token_info, token_refresher);
}
exports.upload_xet_files = upload_xet_files;
async function download_xet_files(repo_type, repo_id, pointer_files) {
    const token_refresher = new TokenRefresher("https://huggingface.co", repo_type, repo_id, "hub_token_TODO", "read", "main");
    let token_info = await token_refresher.refresh_token();
    // return await download_async(files, token_info, token_refresher);
    return [];
}
exports.download_xet_files = download_xet_files;
