import {upload_async, download_async, PointerFile} from "../pkg";

type TokenType = "write" | "read";
type RepoType = "model" | "dataset" | "space";

export class TokenInfo {
    constructor(public readonly tokenType: TokenType, public readonly token: string, public readonly endpoint: string, public readonly expiry: number) {}
}

const HUGGINGFACE_HEADER_X_XET_ENDPOINT = "X-Xet-Cas-Url"
const HUGGINGFACE_HEADER_X_XET_ACCESS_TOKEN = "X-Xet-Access-Token"
const HUGGINGFACE_HEADER_X_XET_EXPIRATION = "X-Xet-Token-Expiration"
const HUGGINGFACE_HEADER_X_XET_HASH = "X-Xet-Hash"
const HUGGINGFACE_HEADER_X_XET_REFRESH_ROUTE = "X-Xet-Refresh-Route"

export class TokenRefresher {
    private readonly endpoint: string;
    private readonly headers: HeadersInit;
    private readonly token_type: TokenType;
    private readonly repo_type: RepoType
    private readonly repo_id: string;
    private readonly revision: string;

    constructor(endpoint: string, repo_type: RepoType, repo_id: string, hub_token: string, token_type: TokenType, revision: string) {
        this.endpoint = endpoint;
        this.repo_type = repo_type;
        this.repo_id = repo_id;
        this.headers = { "Authorization": `Bearer ${hub_token}` };
        this.token_type = token_type;
        this.revision = revision;
    }

    public async refresh_token(): Promise<TokenInfo> {
        const url = `${this.endpoint}/api/${this.repo_type}s/${this.repo_id}/xet-${this.token_type}-token/${this.revision}`
        let token_response: Response;
        try {
            token_response = await fetch(url, { headers: this.headers })
        } catch (e) {
            console.error(`error refreshing token: ${e}`);
            throw e;
        }
        let response_headers = token_response.headers;
        const cas_endpoint: string =  response_headers.get(HUGGINGFACE_HEADER_X_XET_ENDPOINT) ?? "";
        const token: string = response_headers.get(HUGGINGFACE_HEADER_X_XET_ACCESS_TOKEN) ?? "";
        const expiry: number = parseInt(response_headers.get(HUGGINGFACE_HEADER_X_XET_EXPIRATION) ?? "");

        return new TokenInfo(this.token_type, token, cas_endpoint, expiry);
    }
}

export async function upload_xet_files(repo_type: RepoType, repo_id: string, files: File[]): Promise<PointerFile[]> {
    const token_refresher = new TokenRefresher("https://huggingface.co", repo_type, repo_id, "hub_token_TODO", "write", "main");
    let token_info = await token_refresher.refresh_token();
    return await upload_async(files, token_info, token_refresher);
}

export async function download_xet_files(repo_type: RepoType, repo_id: string, pointer_files: PointerFile[]): Promise<Blob[]> {
    const token_refresher = new TokenRefresher("https://huggingface.co", repo_type, repo_id, "hub_token_TODO", "read", "main");
    let token_info = await token_refresher.refresh_token();

    // return await download_async(files, token_info, token_refresher);
    return [];
}