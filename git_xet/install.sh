#!/bin/sh

# This script detects the OS and architecture to download the correct binary,
# unzips it, and moves it to the user's local bin directory.

# --- Configuration ---
URL_LINUX_AMD64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.2.0/git-xet-linux-x86_64.zip"
URL_LINUX_ARM64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.2.0/git-xet-linux-aarch64.zip"
URL_MACOS_AMD64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.2.0/git-xet-macos-x86_64.zip"
URL_MACOS_ARM64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.2.0/git-xet-macos-aarch64.zip"

BINARY_NAME="git-xet"
INSTALL_DIR="/usr/local/bin"

# --- Functions ---

handle_error() {
    echo "Error: $1" >&2
    exit 1
}

# Cleanup function for temp dir
cleanup() {
    echo "Cleaning up..."
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# --- Check required commands ---
for cmd in uname curl unzip; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        handle_error "Required command '$cmd' is not installed. Please install it and rerun this script."
    fi
done

# Detect OS and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

echo "Detected OS: $OS"
echo "Detected Architecture: $ARCH"

DOWNLOAD_URL=""

# Select download URL
case "$OS" in
    Linux)
        case "$ARCH" in
            x86_64) DOWNLOAD_URL="$URL_LINUX_AMD64" ;;
            aarch64|arm64) DOWNLOAD_URL="$URL_LINUX_ARM64" ;;
        esac
        ;;
    Darwin)
        case "$ARCH" in
            x86_64) DOWNLOAD_URL="$URL_MACOS_AMD64" ;;
            arm64) DOWNLOAD_URL="$URL_MACOS_ARM64" ;;
        esac
        ;;
esac

if [ -z "$DOWNLOAD_URL" ]; then
    handle_error "Unsupported OS/Architecture combination: $OS/$ARCH"
fi

# Make temporary directory
TMP_DIR="$(mktemp -d)"
[ -z "$TMP_DIR" ] && handle_error "Failed to create temporary directory."

cd "$TMP_DIR" || handle_error "Could not cd into temp directory."

echo "Downloading from: $DOWNLOAD_URL..."
if ! curl -sSL -o binary.zip "$DOWNLOAD_URL"; then
    handle_error "Download failed."
fi

echo "Unzipping..."
if ! unzip -q binary.zip; then
    handle_error "Unzipping failed. Install 'unzip' and try again."
fi

if [ ! -f "$BINARY_NAME" ]; then
    handle_error "Binary '$BINARY_NAME' not found in the archive."
fi

echo "Setting executable permissions..."
chmod +x "$BINARY_NAME"

echo "Installing to $INSTALL_DIR..."
if [ -w "$INSTALL_DIR" ]; then
    mv "$BINARY_NAME" "$INSTALL_DIR/" || handle_error "Failed to move binary."
else
    echo "Need sudo permissions to install to $INSTALL_DIR."
    if ! sudo mv "$BINARY_NAME" "$INSTALL_DIR/"; then
        handle_error "Failed to move binary with sudo."
    fi
fi

# Post-install
git-xet install --concurrency 3

# Check git-lfs
if ! command -v git-lfs >/dev/null 2>&1; then
    echo "git-lfs is not installed. Please install it for git-xet to work. Install it from https://git-lfs.com/"
fi

echo "Installation complete!"
