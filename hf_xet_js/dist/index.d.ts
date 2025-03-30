import { PointerFile } from "../pkg/hf_xet_js";
export { PointerFile } from "../pkg/hf_xet_js";
type RepoType = "model" | "dataset" | "space";
export declare function upload_xet_files(repo_type: RepoType, repo_id: string, files: Blob[]): Promise<PointerFile[]>;
export declare function download_xet_files(repo_type: RepoType, repo_id: string, pointer_files: PointerFile[]): Promise<Blob[]>;
//# sourceMappingURL=index.d.ts.map