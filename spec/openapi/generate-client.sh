#!/bin/bash

# Script to generate TypeScript API client from OpenAPI specification
# Usage: ./generate-client.sh [output-directory (default: ./generated-client)]

set -e  # Exit on any error

# Configuration
SPEC_FILE="reconstruction_spec.yaml"
DEFAULT_OUTPUT_DIR="./generated-client"
OUTPUT_DIR="${1:-$DEFAULT_OUTPUT_DIR}"
PACKAGE_NAME="cas-reconstruction-client"
PACKAGE_VERSION="1.0.0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 CAS Reconstruction API Client Generator${NC}"
echo "============================================="

# Check if spec file exists
if [ ! -f "$SPEC_FILE" ]; then
    echo -e "${RED}❌ Error: OpenAPI spec file '$SPEC_FILE' not found${NC}"
    echo "Make sure you're running this script from the spec directory"
    exit 1
fi

echo -e "${YELLOW}📋 Configuration:${NC}"
echo "  Spec file: $SPEC_FILE"
echo "  Output directory: $OUTPUT_DIR"
echo "  Package name: $PACKAGE_NAME"
echo ""

# Check if npx is available
if ! command -v npx &> /dev/null; then
    echo -e "${RED}❌ Error: npx is not installed${NC}"
    echo "Please install Node.js and npm first"
    exit 1
fi

# Create output directory
echo -e "${YELLOW}📁 Creating output directory...${NC}"
mkdir -p "$OUTPUT_DIR"

# Generate TypeScript client
echo -e "${YELLOW}🔧 Generating TypeScript client...${NC}"
npx @openapitools/openapi-generator-cli generate \
    -i "$SPEC_FILE" \
    -g typescript-fetch \
    -o "$OUTPUT_DIR" \
    --additional-properties=npmName="$PACKAGE_NAME" \
    --additional-properties=npmVersion="$PACKAGE_VERSION" \
    --additional-properties=supportsES6=true \
    --additional-properties=typescriptThreePlus=true \
    --additional-properties=withoutRuntimeChecks=false \
    --additional-properties=modelPropertyNaming=original \
    --additional-properties=enumPropertyNaming=original

# Create a custom package.json with better configuration
echo -e "${YELLOW}📦 Creating enhanced package.json...${NC}"
cat > "$OUTPUT_DIR/package.json" << EOF
{
  "name": "$PACKAGE_NAME",
  "version": "$PACKAGE_VERSION",
  "description": "TypeScript client for CAS Reconstruction API",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "scripts": {
    "build": "tsc",
    "build:watch": "tsc --watch",
    "clean": "rm -rf dist",
    "test": "jest",
    "lint": "eslint src --ext .ts",
    "lint:fix": "eslint src --ext .ts --fix"
  },
  "keywords": [
    "cas",
    "reconstruction",
    "api",
    "client",
    "typescript",
    "xet"
  ],
  "author": "Generated from OpenAPI spec",
  "license": "MIT",
  "dependencies": {
    "cross-fetch": "^3.1.5"
  },
  "devDependencies": {
    "@types/jest": "^29.5.0",
    "@types/node": "^18.15.0",
    "@typescript-eslint/eslint-plugin": "^5.57.0",
    "@typescript-eslint/parser": "^5.57.0",
    "eslint": "^8.37.0",
    "jest": "^29.5.0",
    "ts-jest": "^29.1.0",
    "typescript": "^5.0.0"
  },
  "files": [
    "dist/**/*",
    "src/**/*",
    "README.md"
  ]
}
EOF

# Create TypeScript configuration
echo -e "${YELLOW}⚙️ Creating TypeScript configuration...${NC}"
cat > "$OUTPUT_DIR/tsconfig.json" << EOF
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "lib": ["ES2020", "DOM"],
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "removeComments": true,
    "moduleResolution": "node",
    "allowSyntheticDefaultImports": true,
    "experimentalDecorators": true,
    "emitDecoratorMetadata": true
  },
  "include": [
    "src/**/*"
  ],
  "exclude": [
    "node_modules",
    "dist"
  ]
}
EOF

# Create a README for the generated client
echo -e "${YELLOW}📝 Creating README...${NC}"
cat > "$OUTPUT_DIR/README.md" << EOF
# CAS Reconstruction API Client

TypeScript client library for the CAS (Content Addressable Storage) Reconstruction API.

## Installation

\`\`\`bash
npm install
\`\`\`

## Building

\`\`\`bash
npm run build
\`\`\`

## Usage

\`\`\`typescript
import { Configuration, ReconstructionApi } from './src';

// Configure the API client
const config = new Configuration({
  basePath: 'http://cas-server.xethub.hf.co:8080',
  headers: {
    'Authorization': 'Bearer YOUR_TOKEN_HERE'
  }
});

const api = new ReconstructionApi(config);

// Get single file reconstruction
async function getFileReconstruction(fileId: string) {
  try {
    const response = await api.getFileReconstruction({
      fileId: fileId
    });
    console.log('Reconstruction data:', response);
    return response;
  } catch (error) {
    console.error('Error getting reconstruction:', error);
    throw error;
  }
}

// Get batch file reconstruction
async function getBatchReconstruction(fileIds: string[]) {
  try {
    const response = await api.batchGetReconstruction({
      fileId: fileIds
    });
    console.log('Batch reconstruction data:', response);
    return response;
  } catch (error) {
    console.error('Error getting batch reconstruction:', error);
    throw error;
  }
}

// Example usage
getFileReconstruction('a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456');

getBatchReconstruction([
  'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456',
  'fedcba0987654321098765432109876543210fedcba098765432109876543'
]);
\`\`\`

## API Reference

### ReconstructionApi

#### \`getFileReconstruction(params)\`
Retrieves reconstruction information for a specific file.

**Parameters:**
- \`fileId\`: string - MerkleHash in hex format (64-character hex string)
- \`range?\`: string - Optional byte range header (format: "bytes=start-end")

**Returns:** \`Promise<QueryReconstructionResponse>\`

#### \`batchGetReconstruction(params)\`
Retrieves reconstruction information for multiple files.

**Parameters:**
- \`fileId\`: string[] - Array of MerkleHashes in hex format

**Returns:** \`Promise<BatchQueryReconstructionResponse>\`

## Types

The client includes full TypeScript type definitions for:
- \`QueryReconstructionResponse\`
- \`BatchQueryReconstructionResponse\`
- \`CASReconstructionTerm\`
- \`CASReconstructionFetchInfo\`
- \`ChunkRange\`
- \`HttpRange\`
- \`HexMerkleHash\`
- \`ErrorResponse\`

## Error Handling

The client will throw errors for:
- 400: Bad Request (invalid parameters)
- 404: File Not Found
- 416: Range Not Satisfiable

All errors include the HTTP status code and error message.

## Generated Code

This client was automatically generated from the OpenAPI specification using OpenAPI Generator.
To regenerate, run: \`./generate-client.sh\` from the spec directory.
EOF

# Create example usage file
echo -e "${YELLOW}💡 Creating example usage file...${NC}"
cat > "$OUTPUT_DIR/example.ts" << EOF
import { Configuration, ReconstructionApi } from './src';

// Example usage of the CAS Reconstruction API client

async function main() {
  // Configure the API client
  const config = new Configuration({
    basePath: 'http://cas-server.xethub.hf.co:8080',
    headers: {
      'Authorization': 'Bearer YOUR_TOKEN_HERE'
    }
  });

  const api = new ReconstructionApi(config);

  try {
    // Example 1: Get single file reconstruction
    console.log('🔍 Getting single file reconstruction...');
    const fileId = 'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456';
    
    const singleResult = await api.getFileReconstruction({
      fileId: fileId
    });
    
    console.log('✅ Single file reconstruction:');
    console.log('  Offset into first range:', singleResult.offsetIntoFirstRange);
    console.log('  Number of terms:', singleResult.terms?.length);
    console.log('  Fetch info keys:', Object.keys(singleResult.fetchInfo || {}));

    // Example 2: Get single file reconstruction with range
    console.log('\\n🎯 Getting file reconstruction with range...');
    const rangeResult = await api.getFileReconstruction({
      fileId: fileId,
      range: 'bytes=0-1023'
    });
    
    console.log('✅ Range reconstruction:');
    console.log('  Offset into first range:', rangeResult.offsetIntoFirstRange);

    // Example 3: Get batch file reconstruction
    console.log('\\n📦 Getting batch file reconstruction...');
    const fileIds = [
      'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456',
      'fedcba0987654321098765432109876543210fedcba098765432109876543'
    ];
    
    const batchResult = await api.batchGetReconstruction({
      fileId: fileIds
    });
    
    console.log('✅ Batch reconstruction:');
    console.log('  Number of files:', Object.keys(batchResult.files || {}).length);
    console.log('  Fetch info keys:', Object.keys(batchResult.fetchInfo || {}));

  } catch (error: any) {
    console.error('❌ Error:', error.message);
    if (error.status) {
      console.error('   Status:', error.status);
    }
  }
}

// Run the example
if (require.main === module) {
  main().catch(console.error);
}
EOF

# Copy spec directory files to generated client
echo -e "${YELLOW}📋 Copying spec directory files...${NC}"
if [ -f "package.json" ]; then
    cp "package.json" "$OUTPUT_DIR/spec-package.json"
    echo "  ✓ Copied package.json to spec-package.json"
fi

if [ -f "README.md" ]; then
    cp "README.md" "$OUTPUT_DIR/spec-README.md"
    echo "  ✓ Copied README.md to spec-README.md"
fi

if [ -f "$SPEC_FILE" ]; then
    cp "$SPEC_FILE" "$OUTPUT_DIR/"
    echo "  ✓ Copied $SPEC_FILE"
fi

echo ""
echo -e "${GREEN}✅ Client generation completed successfully!${NC}"
echo ""
echo -e "${YELLOW}📂 Generated files in: ${OUTPUT_DIR}${NC}"
echo "  📁 src/                    - Generated TypeScript source code"
echo "  📄 package.json            - NPM package configuration"
echo "  📄 tsconfig.json           - TypeScript configuration"
echo "  📄 README.md               - Usage documentation"
echo "  📄 example.ts              - Example usage code"
echo "  📄 spec-package.json       - Original spec directory package.json"
echo "  📄 spec-README.md          - Original spec directory README.md"
echo "  📄 reconstruction_spec.yaml - OpenAPI specification"
echo ""
echo -e "${YELLOW}🚀 Next steps:${NC}"
echo "  1. cd $OUTPUT_DIR"
echo "  2. npm install"
echo "  3. npm run build"
echo "  4. Check example.ts for usage patterns"
echo ""
echo -e "${GREEN}🎉 Happy coding!${NC}" 