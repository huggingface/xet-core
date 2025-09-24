#!/bin/bash

# This script detects the OS and architecture to download the correct binary,
# unzips it, and moves it to the user's local bin directory.

# --- Configuration ---
URL_LINUX_AMD64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.1.0/git-xet-linux-x86_64.zip"
URL_LINUX_ARM64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.1.0/git-xet-linux-aarch64.zip"
URL_MACOS_AMD64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.1.0/git-xet-macos-x86_64.zip"
URL_MACOS_ARM64="https://github.com/huggingface/xet-core/releases/download/git-xet-v0.1.0/git-xet-macos-aarch64.zip"

# The name of the binary inside the zip file.
BINARY_NAME="git-xet"

# The destination for the binary.
INSTALL_DIR="/usr/local/bin"

# --- Main Script ---

# Function to handle errors and exit
handle_error() {
    echo "Error: $1" >&2
    exit 1
}

# Get OS and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

echo "Detected OS: ${OS}"
echo "Detected Architecture: ${ARCH}"

# Determine which URL to use
DOWNLOAD_URL=""
case "${OS}" in
    Linux)
        case "${ARCH}" in
            x86_64)
                DOWNLOAD_URL="${URL_LINUX_AMD64}"
                ;;
            aarch64 | arm64)
                DOWNLOAD_URL="${URL_LINUX_ARM64}"
                ;;
        esac
        ;;
    Darwin) # macOS
        case "${ARCH}" in
            x86_64)
                DOWNLOAD_URL="${URL_MACOS_AMD64}"
                ;;
            arm64)
                DOWNLOAD_URL="${URL_MACOS_ARM64}"
                ;;
        esac
        ;;
esac

# Check if a download URL was found
if [ -z "${DOWNLOAD_URL}" ]; then
    handle_error "Unsupported OS/Architecture combination: ${OS}/${ARCH}"
fi

# Create a temporary directory for the download and extraction
TMP_DIR=$(mktemp -d)

# Function to clean up the temporary directory on exit
cleanup() {
    echo "Cleaning up..."
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

# Change to the temporary directory
cd "${TMP_DIR}" || handle_error "Could not change to temporary directory."

# Download the file
echo "Downloading from: ${DOWNLOAD_URL}..."
curl -sSL -o binary.zip "${DOWNLOAD_URL}" || handle_error "Download failed."

# Unzip the file
echo "Unzipping..."
unzip -q binary.zip || handle_error "Unzipping failed. Make sure 'unzip' is installed."

# Check if the binary exists
if [ ! -f "${BINARY_NAME}" ]; then
    handle_error "Binary '${BINARY_NAME}' not found in the zip file."
fi

# Make the binary executable
echo "Setting executable permissions..."
chmod +x "${BINARY_NAME}"

# Move the binary to the install directory
echo "Installing binary to ${INSTALL_DIR}..."
# Use sudo if the user doesn't have write permissions to the directory
if [ -w "${INSTALL_DIR}" ]; then
    mv "${BINARY_NAME}" "${INSTALL_DIR}/" || handle_error "Failed to move binary."
else
    echo "This script requires sudo permissions to install to ${INSTALL_DIR}."
    sudo mv "${BINARY_NAME}" "${INSTALL_DIR}/" || handle_error "Failed to move binary with sudo."
fi

git-xet install --concurrency 3

echo "Installation complete!"
