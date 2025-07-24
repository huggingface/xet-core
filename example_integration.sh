#!/bin/bash

# Example: Integrating XET-Core WASM into your project
# This shows how you might use the build_xet_wasm.sh script in your own project

set -e

PROJECT_ROOT="$(pwd)"
WASM_OUTPUT_DIR="$PROJECT_ROOT/src/wasm"
BUILD_SCRIPT_URL="https://raw.githubusercontent.com/huggingface/xet-core/main/build_xet_wasm.sh"

echo "🚀 Setting up XET-Core WASM for your project..."

# Download the build script if it doesn't exist
if [[ ! -f "build_xet_wasm.sh" ]]; then
    echo "📥 Downloading WASM build script..."
    curl -o build_xet_wasm.sh "$BUILD_SCRIPT_URL"
    chmod +x build_xet_wasm.sh
fi

# Build WASM and copy to your project
echo "🔨 Building WASM package..."
./build_xet_wasm.sh -t web -o "$WASM_OUTPUT_DIR"

echo "📦 WASM package ready in: $WASM_OUTPUT_DIR"
echo ""
echo "Next steps:"
echo "1. Import the WASM module in your JavaScript/TypeScript code"
echo "2. Example usage:"
echo ""
echo "   import init from './src/wasm/hf_xet_thin_wasm.js';"
echo ""
echo "   async function setupWasm() {"
echo "     await init();"
echo "     // Use WASM functions here"
echo "   }"
echo ""
echo "✅ Done! Your WASM package is ready to use." 