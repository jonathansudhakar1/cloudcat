#!/bin/bash
set -e

# Build standalone cloudcat executable for local testing
#
# Usage:
#   ./scripts/build-standalone.sh           # Build only
#   ./scripts/build-standalone.sh --package # Build and create tarball
#   ./scripts/build-standalone.sh --clean   # Clean build artifacts

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/packaging/pyinstaller"
VENV_DIR="$BUILD_DIR/.venv"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() { echo -e "${GREEN}==>${NC} $1"; }
echo_warn() { echo -e "${YELLOW}Warning:${NC} $1"; }
echo_error() { echo -e "${RED}Error:${NC} $1"; }

# Handle --clean flag
if [ "$1" == "--clean" ]; then
    echo_info "Cleaning build artifacts..."
    rm -rf "$BUILD_DIR/build" "$BUILD_DIR/dist" "$VENV_DIR"
    echo_info "Clean complete"
    exit 0
fi

echo_info "Building cloudcat standalone executable"
echo "Project root: $PROJECT_ROOT"
echo "Build directory: $BUILD_DIR"
echo ""

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo_error "python3 is required but not installed"
    exit 1
fi

PYTHON_VERSION=$(python3 --version)
echo_info "Using $PYTHON_VERSION"

# Create virtual environment for clean build
if [ ! -d "$VENV_DIR" ]; then
    echo_info "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Install dependencies
echo_info "Installing dependencies..."
pip install --upgrade pip --quiet
pip install pyinstaller --quiet
pip install -e "$PROJECT_ROOT[all]" --quiet

# Build
echo_info "Building with PyInstaller..."
cd "$BUILD_DIR"
pyinstaller cloudcat.spec --noconfirm --clean

# Test the build (now in dist/cloudcat/ directory)
echo_info "Testing build..."
if ./dist/cloudcat/cloudcat --help > /dev/null 2>&1; then
    echo_info "Build test passed - executable runs successfully"
else
    echo_error "Build test failed - executable does not run"
    deactivate
    exit 1
fi

# Get version and architecture
VERSION=$(python -c "from cloudcat import __version__; print(__version__)")
ARCH=$(uname -m)

echo ""
echo_info "Build complete!"
echo "  Executable: $BUILD_DIR/dist/cloudcat/cloudcat"
echo "  Version:    $VERSION"
echo "  Arch:       $ARCH"

# Create tarball if requested
if [ "$1" == "--package" ]; then
    echo ""
    echo_info "Creating distribution package..."
    cd dist
    TARBALL="cloudcat-${VERSION}-macos-${ARCH}.tar.gz"
    tar -czvf "$TARBALL" cloudcat

    echo ""
    echo_info "Package created!"
    echo "  File: $BUILD_DIR/dist/$TARBALL"
    echo "  SHA256: $(shasum -a 256 "$TARBALL" | cut -d' ' -f1)"
fi

deactivate

echo ""
echo_info "To test the executable:"
echo "  $BUILD_DIR/dist/cloudcat/cloudcat --help"
echo "  $BUILD_DIR/dist/cloudcat/cloudcat -p gcs://your-bucket/file.csv"
