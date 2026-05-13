#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 默认模式
MODE="${1:-debug}"
BUILD_DIR="build/${MODE}"

print_usage() {
    echo "Usage: $0 [debug|release|test|clean]"
    echo "  debug   - Build with debug symbols and AddressSanitizer (default)"
    echo "  release - Build with O2 optimization"
    echo "  test    - Build and run all tests"
    echo "  clean   - Remove all build artifacts"
}

case "$MODE" in
    debug)
        echo "=== Building in DEBUG mode (with ASan) ==="
        mkdir -p "$BUILD_DIR"
        cmake -B "$BUILD_DIR" -S . -DCMAKE_BUILD_TYPE=Debug
        cmake --build "$BUILD_DIR" -j$(nproc)
        echo "=== Build complete: output/bin/debug/ ==="
        ;;
    release)
        echo "=== Building in RELEASE mode ==="
        mkdir -p "$BUILD_DIR"
        cmake -B "$BUILD_DIR" -S . -DCMAKE_BUILD_TYPE=Release
        cmake --build "$BUILD_DIR" -j$(nproc)
        echo "=== Build complete: output/bin/release/ ==="
        ;;
    test)
        echo "=== Building and running tests ==="
        mkdir -p "$BUILD_DIR"
        cmake -B "$BUILD_DIR" -S . -DCMAKE_BUILD_TYPE=Debug
        cmake --build "$BUILD_DIR" -j$(nproc)
        cd "$BUILD_DIR"
        ctest --output-on-failure
        ;;
    clean)
        echo "=== Cleaning build artifacts ==="
        rm -rf build output
        echo "=== Clean complete ==="
        ;;
    *)
        print_usage
        exit 1
        ;;
esac
