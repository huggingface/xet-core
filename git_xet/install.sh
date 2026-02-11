#!/bin/sh

# This script detects the OS and architecture to download the correct binary,
# unzips it, and moves it to the user's local bin directory.

# --- Configuration ---
XET_LINUX_AMD64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.2.0/git-xet-linux-x86_64.zip"
XET_LINUX_ARM64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.2.0/git-xet-linux-aarch64.zip"
XET_MACOS_AMD64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.2.0/git-xet-macos-x86_64.zip"
XET_MACOS_ARM64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.2.0/git-xet-macos-aarch64.zip"

LFS_LINUX_AMD64="https://github.com/git-lfs/git-lfs/releases/download/v3.7.1/git-lfs-linux-amd64-v3.7.1.tar.gz"
LFS_LINUX_ARM64="https://github.com/git-lfs/git-lfs/releases/download/v3.7.1/git-lfs-linux-arm64-v3.7.1.tar.gz"
LFS_MACOS_AMD64="https://github.com/git-lfs/git-lfs/releases/download/v3.7.1/git-lfs-darwin-amd64-v3.7.1.zip"
LFS_MACOS_ARM64="https://github.com/git-lfs/git-lfs/releases/download/v3.7.1/git-lfs-darwin-arm64-v3.7.1.zip"

BINARY_NAME="git-xet"
INSTALL_DIR="/usr/local/bin"

LFS_DIR="git-lfs-3.7.1"

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

XET_URL=""
LFS_URL=""

# Select download URL
case "$OS" in
    Linux)
        case "$ARCH" in
            x86_64)
                XET_URL="$XET_LINUX_AMD64"
                LFS_URL="$LFS_LINUX_AMD64"
                ;;
            aarch64|arm64)
                XET_URL="$XET_LINUX_ARM64"
                LFS_URL="$LFS_LINUX_ARM64"
                ;;
        esac
        ;;
    Darwin)
        case "$ARCH" in
            x86_64)
                XET_URL="$XET_MACOS_AMD64"
                LFS_URL="$LFS_MACOS_AMD64"
                ;;
            arm64)
                XET_URL="$XET_MACOS_ARM64"
                LFS_URL="$LFS_MACOS_ARM64"
                ;;
        esac
        ;;
esac

if [ -z "$XET_URL" ]; then
    handle_error "Unsupported OS/Architecture combination: $OS/$ARCH"
fi

# Make temporary directory
TMP_DIR="$(mktemp -d)"
[ -z "$TMP_DIR" ] && handle_error "Failed to create temporary directory."

cd "$TMP_DIR" || handle_error "Could not cd into temp directory."

echo "Downloading from: $XET_URL..."
if ! curl -sSL -o binary.zip "$XET_URL"; then
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

# Check git-lfs
if ! command -v git-lfs >/dev/null 2>&1; then
    printf "The dependency git-lfs is not installed. Continue to install it from https://github.com/git-lfs/git-lfs/releases? (y/n) "
    read -r response < /dev/tty
    if [ "$response" = "y" ]; then
        # Download and extract git-lfs based on OS
        if [ "$OS" = "Linux" ]; then
            echo "Downloading git-lfs from: $LFS_URL..."
            if ! curl -sSL -o lfs.tar.gz "$LFS_URL"; then
                handle_error "LFS download failed."
            fi
            echo "Extracting tarball..."
            if ! tar -xzf lfs.tar.gz; then
                handle_error "LFS extraction failed. Install 'tar' and try again."
            fi
        else # Darwin (macOS)
            echo "Downloading git-lfs from: $LFS_URL..."
            if ! curl -sSL -o lfs.zip "$LFS_URL"; then
                handle_error "LFS download failed."
            fi
            echo "Unzipping..."
            if ! unzip -q lfs.zip; then
                handle_error "Unzipping LFS failed. Install 'unzip' and try again."
            fi
        fi
        cd "$LFS_DIR" || handle_error "Could not cd into LFS directory '$LFS_DIR'."
        sudo ./install.sh
    else
        echo "Please install git-lfs for git-xet to work. Install it from https://git-lfs.com/"
    fi
fi

# Check for OpenSSL 3 dependency on macOS
OPENSSL_AVAILABLE=true
if [ "$OS" = "Darwin" ]; then
    OPENSSL_LIB="/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib"
    if [ "$ARCH" = "x86_64" ]; then
        OPENSSL_LIB="/usr/local/opt/openssl@3/lib/libssl.3.dylib"
    fi
    if [ ! -f "$OPENSSL_LIB" ]; then
        OPENSSL_AVAILABLE=false
        echo ""
        echo "WARNIN: OpenSSL 3 is required but was not found at: $OPENSSL_LIB"
        echo "The git-xet binary is dynamically linked against OpenSSL 3."
        echo ""
        if command -v brew >/dev/null 2>&1; then
            printf "Install it now with Homebrew? (y/n) "
            read -r response < /dev/tty
            if [ "$response" = "y" ]; then
                if brew install openssl@3; then
                    OPENSSL_AVAILABLE=true
                else
                    handle_error "Failed to install OpenSSL 3 via Homebrew."
                fi
            else
                echo "Please run 'brew install openssl@3' manually before using git-xet."
            fi
        else
            echo "Please install Homebrew (https://brew.sh) and then run:"
            echo "  brew install openssl@3"
            echo ""
            echo "Alternatively, install OpenSSL 3 so that $OPENSSL_LIB exists."
        fi
    fi
fi

# Post-install
if [ "$OPENSSL_AVAILABLE" = false ]; then
    echo ""
    echo "Skipping 'git-xet install' because OpenSSL 3 is not available."
    echo "After installing OpenSSL 3, run:"
    echo "  brew install openssl@3"
    echo "  git-xet install --concurrency 3"
    exit 1
elif ! git-xet install --concurrency 3; then
    echo ""
    echo "WARNING: 'git-xet install' failed."
    if [ "$OS" = "Darwin" ]; then
        echo "This might be due to a missing OpenSSL 3 dependency."
        echo "Run 'brew install openssl@3' and then 'git-xet install --concurrency 3'."
    fi
    exit 1
fi

echo "Installation complete!"
