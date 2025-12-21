#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DOCKER_IMAGE_NAME="xet-adaptive-concurrency-test"
PROJECT_ROOT="/Users/hoytak/workspace/hf/xet-core-2"
TEST_DIR="cas_client/test/adaptive_concurrency"
DOCKER_DIR="$PROJECT_ROOT/$TEST_DIR/docker"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_status "Building Docker container for adaptive concurrency testing..."

# Remove existing image if it exists
if docker images -q $DOCKER_IMAGE_NAME | grep -q .; then
    print_status "Removing existing Docker image..."
    docker rmi $DOCKER_IMAGE_NAME > /dev/null 2>&1 || true
fi

# Build the image
print_status "Building Docker image..."
cd "$DOCKER_DIR"
docker build -t "$DOCKER_IMAGE_NAME" . || {
    print_error "Failed to build Docker image"
    exit 1
}

print_success "Docker image built successfully"
print_status "Image name: $DOCKER_IMAGE_NAME"
print_status "You can now run scenarios using run_scenarios.sh"
