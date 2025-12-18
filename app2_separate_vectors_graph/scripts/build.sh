#!/bin/bash

# Usage: ./scripts/build.sh [all|splitter|graph_extractor|graph_merger]
TARGET=$1

# Define our functions
functions=("splitter" "graph_extractor" "graph_merger")

build_func() {
    name=$1
    echo "🏗️  Building $name..."
    # We tag them consistently as 'app2-splitter', etc.
    podman build -t app2-$name -f $name/Containerfile $name
}

if [ "$TARGET" == "all" ] || [ -z "$TARGET" ]; then
    for func in "${functions[@]}"; do
        build_func $func
    done
else
    # Check if directory exists
    if [ -d "$TARGET" ]; then
        build_func $TARGET
    else
        echo "❌ Error: Directory '$TARGET' not found."
        echo "Available functions: ${functions[*]}"
        exit 1
    fi
fi

echo "✅ Build complete!"