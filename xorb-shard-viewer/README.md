# XORB & Shard File Viewer

A static web application built with Svelte that allows you to upload and analyze XORB or Shard object files to view their metadata structure.

## Features

- **File Upload**: Drag & drop or click to upload XORB and Shard files
- **Automatic Detection**: Automatically detects file type based on binary structure
- **Comprehensive Metadata Display**: Shows detailed information about file contents
- **Responsive Design**: Works on desktop and mobile devices
- **Error Handling**: Provides clear error messages for invalid files

## Supported File Types

### 📦 XORB Files

XORB (Xet Orb) files contain collections of compressed chunks with metadata. The viewer displays:

- Chunk count and sizes
- Compression ratios
- Hash information
- Boundary offsets
- Individual chunk details

### 🗂️ Shard Files

MDB Shard files store file metadata and content-addressable storage information for efficient deduplication. The viewer shows:

- File information entries
- CAS (Content Addressable Storage) data
- Lookup tables
- Timestamps and security keys
- Header and footer details

## Development

### Prerequisites

- Node.js 18+
- pnpm (or npm/yarn)

### Setup

```bash
cd xorb-shard-viewer
pnpm install
pnpm run dev
```

The application will be available at `http://localhost:5173`

### Build for Production

```bash
pnpm run build
```

The static files will be generated in the `build` directory.

## File Format Support

The application implements binary parsers for:

- **XORB Format**: Based on the CAS object format with chunk compression and merkle hashing
- **Shard Format**: MDB shard file format v2 with header, footer, file info, and CAS info sections

## Architecture

- **Frontend**: Svelte + TypeScript
- **Parsing**: Custom binary parsers for both file formats
- **Styling**: Component-scoped CSS with responsive design
- **Type Safety**: Full TypeScript support with detailed type definitions

## Browser Compatibility

Works in all modern browsers that support:

- File API
- ArrayBuffer/Uint8Array
- ES2020+ features

## Security

- All file processing happens client-side
- No data is uploaded to any server
- Files are processed entirely in the browser's memory

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

See the parent repository's LICENSE file for licensing information.
