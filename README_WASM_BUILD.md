# XET-Core WASM Builder

This script automatically clones the xet-core repository and builds WASM packages with all necessary dependencies.

## Quick Start

```bash
# Basic usage - builds hf_xet_thin_wasm from main branch
./build_xet_wasm.sh

# Build and copy to your project
./build_xet_wasm.sh -o ./my-project/wasm

# Build for Node.js
./build_xet_wasm.sh -t nodejs -o ./dist
```

## Prerequisites

- **Git**: For cloning the repository
- **Rust**: Install from [rustup.rs](https://rustup.rs/) - the script will automatically set up the nightly toolchain and WASM components
- **Internet connection**: For downloading dependencies

## Usage

```bash
./build_xet_wasm.sh [OPTIONS]
```

### Options

- `-b, --branch BRANCH`: Git branch to checkout (default: main)
- `-p, --package PACKAGE`: WASM package to build - `hf_xet_thin_wasm` or `hf_xet_wasm` (default: hf_xet_thin_wasm)
- `-t, --target TARGET`: JavaScript target - `web`, `nodejs`, `bundler`, `no-modules`, `deno` (default: web)
- `-o, --output DIR`: Output directory to copy built WASM files
- `-c, --clean`: Clean clone directory before starting
- `-h, --help`: Show help message

### Examples

```bash
# Build default package (hf_xet_thin_wasm) from main branch
./build_xet_wasm.sh

# Build from a specific branch
./build_xet_wasm.sh -b feature-branch

# Build the full WASM package (more features, larger size)
./build_xet_wasm.sh -p hf_xet_wasm

# Build for Node.js and copy to dist directory
./build_xet_wasm.sh -t nodejs -o ./dist

# Clean previous build and start fresh
./build_xet_wasm.sh -c

# Build and copy to your project's WASM directory
./build_xet_wasm.sh -o ./my-project/src/wasm
```

## Package Differences

### hf_xet_thin_wasm (Default)
- Smaller size
- Simpler build process
- Uses standard `wasm-pack` build
- Good for most use cases

### hf_xet_wasm
- Full feature set
- Larger size
- Complex build with nightly features
- Includes additional functionality

## Output

The script generates:
- `.wasm` file: The compiled WebAssembly binary
- `.js` file: JavaScript bindings
- `.ts` file: TypeScript definitions
- `package.json`: NPM package metadata

## Integration

After building, you can integrate the WASM package into your project:

### Web/Bundler
```javascript
import init, { YourWasmFunction } from './path/to/wasm/package';

async function run() {
    await init();
    // Use WASM functions
}
```

### Node.js
```javascript
const wasm = require('./path/to/wasm/package');

async function run() {
    await wasm.default();
    // Use WASM functions
}
```

## Troubleshooting

### Common Issues

1. **Rust not installed**: Install from [rustup.rs](https://rustup.rs/)
2. **Network issues**: Ensure internet connection for downloading dependencies
3. **Permission denied**: Make sure the script is executable (`chmod +x build_xet_wasm.sh`)
4. **Build failures**: Try cleaning with `-c` flag

### Getting Help

- Use `./build_xet_wasm.sh -h` for built-in help
- Check the [xet-core repository](https://github.com/huggingface/xet-core) for issues
- Look at the CI configuration in `.github/workflows/ci.yml` for reference

## Advanced Usage

The script automatically handles:
- Installing Rust nightly toolchain
- Adding WASM target
- Installing `wasm-pack` and `wasm-bindgen-cli`
- Setting up build environment
- Running the appropriate build process

For custom builds, you can modify the cloned repository in `xet-core-wasm-build/` directory after the script runs. 