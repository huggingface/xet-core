import { upload_async, download_async, PointerFile as WasmPointerFile } from "hf_xet_js";


type TokenType = "write" | "read";
type RepoType = "model" | "dataset" | "space";

const HUGGINGFACE_HEADER_X_XET_ENDPOINT = "X-Xet-Cas-Url";
const HUGGINGFACE_HEADER_X_XET_ACCESS_TOKEN = "X-Xet-Access-Token";
const HUGGINGFACE_HEADER_X_XET_EXPIRATION = "X-Xet-Token-Expiration";
const HUGGINGFACE_HEADER_X_XET_HASH = "X-Xet-Hash";
const HUGGINGFACE_HEADER_X_XET_REFRESH_ROUTE = "X-Xet-Refresh-Route";

class TokenInfo {
	constructor(
		private readonly _token: string,
		private readonly _endpoint: string,
		private readonly _expiry: BigInt,
	) {
	}

	public get token() {
		return this._token;
	}

	public get endpoint() {
		return this._endpoint;
	}

	public get expiry() {
		return this._expiry;
	}
}

class TokenRefresher {
	private readonly endpoint: string;
	private readonly headers: HeadersInit;
	private readonly token_type: TokenType;
	private readonly repo_type: RepoType;
	private readonly repo_id: string;
	private readonly revision: string;

	constructor(
		endpoint: string,
		repo_type: RepoType,
		repo_id: string,
		hub_token: string,
		token_type: TokenType,
		revision: string,
	) {
		this.endpoint = endpoint;
		this.repo_type = repo_type;
		this.repo_id = repo_id;
		this.headers = { Authorization: `Bearer ${hub_token}` };
		this.token_type = token_type;
		this.revision = revision;
	}

	public async refresh_token(): Promise<TokenInfo> {
		const url = `${this.endpoint}/api/${this.repo_type}s/${this.repo_id}/xet-${this.token_type}-token/${this.revision}`;
		let token_response: Response;
		try {
			token_response = await fetch(url, { headers: this.headers });
		} catch (e) {
			console.error(`error refreshing token: ${e}`);
			throw e;
		}
		let response_headers = token_response.headers;
		const cas_endpoint: string = response_headers.get(HUGGINGFACE_HEADER_X_XET_ENDPOINT) ?? "";
		const token: string = response_headers.get(HUGGINGFACE_HEADER_X_XET_ACCESS_TOKEN) ?? "";
		const expiry: number = parseInt(response_headers.get(HUGGINGFACE_HEADER_X_XET_EXPIRATION) ?? "") ?? 0;

		return new TokenInfo(token, cas_endpoint, BigInt(isNaN(expiry) ? 0 : expiry));
	}
}

export class PointerFile {
	constructor(public readonly hash: string, public readonly size: number, public readonly sha256: string) {
	}
}

function convertPointerFile(pf: WasmPointerFile): PointerFile {
	return new PointerFile(pf.hash, pf.size, pf.sha256);
}

export async function uploadXetFiles(repoType: RepoType, repoId: string, files: Blob[]): Promise<PointerFile[]> {
	const tokenRefresher = new TokenRefresher(
		"https://huggingface.co",
		repoType,
		repoId,
		"hub_token_TODO",
		"write",
		"main",
	);
	let initialTokenInfo = await tokenRefresher.refresh_token();
	return await upload_async(files, initialTokenInfo, tokenRefresher);
}

export async function download_xet_files(
	repo_type: RepoType,
	repo_id: string,
	pointer_files: PointerFile[],
): Promise<Blob[]> {
	const token_refresher = new TokenRefresher(
		"https://huggingface.co",
		repo_type,
		repo_id,
		"hub_token_TODO",
		"read",
		"main",
	);
	let token_info = await token_refresher.refresh_token();

	// return await download_async(files, token_info, token_refresher);
	return [];
}
