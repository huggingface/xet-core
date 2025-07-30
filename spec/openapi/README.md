# CAS Reconstruction API Specification

This directory contains the OpenAPI specification for the CAS (Content Addressable Storage) Reconstruction API and tools for generating client code.

## Files

- `reconstruction_spec.yaml` - Complete OpenAPI 3.0.3 specification
- `api.md` - Human-readable API documentation
- `generate-client.sh` - Script to generate TypeScript client code
- `package.json` - NPM configuration for spec management tools

## Quick Start

### 1. Generate TypeScript Client

```bash
# Generate client in default location (./generated-client)
./generate-client.sh

# Generate client in custom location
./generate-client.sh ./my-custom-output-dir
```

### 2. Use Generated Client

```bash
cd ./generated-client  # or your custom directory
npm install
npm run build
```

### 3. Import and Use in Your Project

```typescript
import { Configuration, ReconstructionApi } from './path/to/generated-client/src';

const config = new Configuration({
  basePath: 'http://cas-server.xethub.hf.co:8080',
  headers: {
    'Authorization': 'Bearer YOUR_TOKEN_HERE'
  }
});

const api = new ReconstructionApi(config);

// Get single file reconstruction
const result = await api.getFileReconstruction({
  fileId: 'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456'
});
```

## Advanced Usage

### Validate Specification

```bash
npm run lint-spec
```

### Preview Documentation

```bash
# Start documentation server on http://localhost:8081
npm run serve-docs
```

### Bundle Specification

```bash
# Create a single bundled YAML file
npm run bundle
```

## API Endpoints

### GET `/reconstruction/{file_id}`

Retrieves reconstruction information for a specific file.

**Parameters:**

- `file_id` (path): MerkleHash in hex format (64 characters)
- `Range` (header, optional): Byte range `bytes=start-end`

**Responses:**

- `200`: `QueryReconstructionResponse`
- `400`: Bad request
- `404`: File not found
- `416`: Range not satisfiable

## Generated Client Features

The generated TypeScript client includes:

✅ **Full Type Safety** - Complete TypeScript definitions  
✅ **Fetch API** - Modern HTTP client using fetch  
✅ **Promise-based** - Async/await support  
✅ **Error Handling** - Proper HTTP error handling  
✅ **Bearer Auth** - Built-in authentication support  
✅ **Range Requests** - Support for byte range headers  
✅ **Multiple Environments** - Configurable base URLs  

## Types

The specification defines these main types:

- `QueryReconstructionResponse` - Single file reconstruction info
- `BatchQueryReconstructionResponse` - Multiple files reconstruction info
- `CASReconstructionTerm` - Individual reconstruction term
- `CASReconstructionFetchInfo` - URL and range information for fetching
- `ChunkRange` - Chunk index range (exclusive end)
- `HttpRange` - Byte range (inclusive end)
- `HexMerkleHash` - 64-character hex string
- `ErrorResponse` - Standard error format

## Examples

### Single File Reconstruction

```typescript
const response = await api.getFileReconstruction({
  fileId: 'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456'
});

console.log('Offset:', response.offsetIntoFirstRange);
console.log('Terms:', response.terms?.length);
```

### Range Request

```typescript
const response = await api.getFileReconstruction({
  fileId: 'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456',
  range: 'bytes=0-1023'
});
```

## Customization

### Different Client Generators

The script uses `typescript-fetch` generator. To use different generators:

```bash
# Axios-based client
npx @openapitools/openapi-generator-cli generate \
  -i reconstruction_spec.yaml \
  -g typescript-axios \
  -o ./axios-client

# Node.js client
npx @openapitools/openapi-generator-cli generate \
  -i reconstruction_spec.yaml \
  -g typescript-node \
  -o ./node-client
```

### Custom Configuration

Edit the `additional-properties` in `generate-client.sh` for different options:

- `modelPropertyNaming`: `camelCase`, `PascalCase`, `snake_case`, `original`
- `enumPropertyNaming`: `camelCase`, `PascalCase`, `UPPERCASE`, `original`
- `typescriptThreePlus`: `true` for TypeScript 3+ features
- `withoutRuntimeChecks`: `false` to include runtime validation

## Troubleshooting

### Generator Fails

1. Ensure Node.js and npm are installed
2. Check OpenAPI spec validity: `npm run lint-spec`
3. Clear npm cache: `npm cache clean --force`

### Types Don't Match

1. Regenerate client: `./generate-client.sh`
2. Check specification changes in `reconstruction_spec.yaml`
3. Verify server response format matches spec

### Authentication Issues

1. Verify bearer token format
2. Check server CORS configuration
3. Ensure proper headers in Configuration

## Contributing

When updating the API:

1. Modify `reconstruction_spec.yaml`
2. Validate: `npm run lint-spec`
3. Regenerate client: `./generate-client.sh`
4. Update documentation as needed
5. Test with `example.ts` in generated client

## Tools Used

- **OpenAPI Generator** - Client code generation
- **Swagger Parser** - Specification validation
- **Redocly CLI** - Documentation and bundling
