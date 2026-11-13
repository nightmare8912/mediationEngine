#!/usr/bin/env bash

set -euo pipefail

# Resolve the project root from this script's location, so it works
# regardless of the directory from which it is invoked.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${MEDIATION_INSTALL_DIR:-$HOME/Desktop/MediationEngine}"
SOURCE_CONFIG_DIR="$PROJECT_DIR/src/main/resources/configs"
SOURCE_PATH_CONFIG="$PROJECT_DIR/src/main/resources/installer/allFilePath.conf"
TARGET_CONFIG_DIR="$INSTALL_DIR/conf"

if [[ ! -d "$SOURCE_CONFIG_DIR" ]]; then
    echo "ERROR: Configuration source directory not found: $SOURCE_CONFIG_DIR" >&2
    exit 1
fi

if [[ ! -f "$SOURCE_PATH_CONFIG" ]]; then
    echo "ERROR: allFilePath.conf not found: $SOURCE_PATH_CONFIG" >&2
    exit 1
fi

if [[ ! -d "$INSTALL_DIR" ]]; then
    echo "ERROR: Installation directory not found: $INSTALL_DIR" >&2
    echo "Create the installation directory first, or set MEDIATION_INSTALL_DIR." >&2
    exit 1
fi

mkdir -p "$TARGET_CONFIG_DIR/installer"

# Copy the external configuration tree without touching data/, logs/, or the JAR.
cp -R "$SOURCE_CONFIG_DIR/." "$TARGET_CONFIG_DIR/"
cp "$SOURCE_PATH_CONFIG" "$TARGET_CONFIG_DIR/installer/allFilePath.conf"

# Remove Finder metadata if it was copied from the source tree.
find "$TARGET_CONFIG_DIR" -name '.DS_Store' -delete

echo "Configuration deployment successful."
echo "Source:      $SOURCE_CONFIG_DIR"
echo "Destination: $TARGET_CONFIG_DIR"
echo ""
find "$TARGET_CONFIG_DIR" -type f -print | sort
