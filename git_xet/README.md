Git-Xet is a Git LFS custom transfer agent that implements upload and download of files using the Xet protocol. Install `git-xet`, follow your regular workflow to `git lfs track ...` & `git add ...` & `git commit ...` & `git push`, and your files are uploaded to Hugging Face repos using the Xet protocol. Enjoy the dedupe!

## Installation
### Prerequisite
Make sure you have [git](https://git-scm.com/downloads) and [git-lfs](https://git-lfs.com/) installed and configured correctly.
### macOS or Linux (amd64 or aarch64)
 To install using Homebrew:
   ```
   brew tap huggingface/tap
   brew install git-xet
   ```
 Or, using an installation script, run the following in your terminal (requires `curl` and `unzip`):
   ```
   curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/huggingface/xet-core/refs/heads/main/git_xet/install.sh | sh
   ```
  To verify the installation, run:
   ```
   git-xet --version
   ```

### Windows (amd64)
 Using an installer: 
 - Download `git-xet-windows-installer-x86_64.zip` ([available here](https://github.com/huggingface/xet-core/releases/download/git-xet-v0.1.0/git-xet-windows-installer-x86_64.zip)) and unzip. 
 - Run the `msi` installer file and follow the prompts.
   
 Manual installation:
 - Download `git-xet-windows-x86_64.zip` ([available here](https://github.com/huggingface/xet-core/releases/download/git-xet-v0.1.0/git-xet-windows-x86_64.zip)) and unzip. 
 - Place the extracted `git-xet.exe` under a `PATH` directory.
 - Run `git-xet install` in a terminal.

To verity the installation, run:
  ```
  git-xet --version
  ```

## Uninstall
### macOS & Linux
Using Homebrew:
   ```
   git-xet uninstall
   brew uninstall git-xet
   ```
If you used the installation script (for MacOS or Linux), run the following in your terminal:
   ```
   git-xet uninstall
   sudo rm $(which git-xet)
   ```
### Windows
If you used the installer:
-  Navigate to Settings -> Apps -> Installed apps
- Find "Git-Xet".
- Select the "Uninstall" option available in the context menu.

If you manually installed:
- Run `git-xet uninstall` in a terminal. 
- Delete the `git-xet.exe` file from the location where it was originally placed.

## How It Works
Git-Xet works by registering itself as a custom transfer agent to Git LFS by name "xet". On `git push`, `git fetch` or `git pull`, `git-lfs` negotiates with the remote server to determine the transfer agent to use. During this process, `git-lfs` sends to the server all locally registered agent names in the Batch API request, and the server replies with exactly one agent name in the response. Should "xet" be picked, `git-lfs` delegates the uploading or downloading operation to `git-xet` through a sequential protocol.

For more details, see the Git LFS [Batch API](https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md) and [Custom Transfer Agent](https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md) documentation.
