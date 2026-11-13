#!/bin/bash

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="$HOME/Desktop/MediationEngine"

echo "=================================="
echo " Building Mediation Engine"
echo "=================================="

cd "$PROJECT_DIR"

./gradlew clean shadowJar

JAR="$PROJECT_DIR/build/libs/mediation-engine.jar"

if [[ ! -f "$JAR" ]]; then
    echo "ERROR: Build artifact not found."
    exit 1
fi

if [[ ! -d "$INSTALL_DIR" ]]; then
    echo "ERROR: Installation directory not found."
    exit 1
fi

echo "=================================="
echo " Deploying JAR"
echo "=================================="

cp -f "$JAR" "$INSTALL_DIR/mediation-engine.jar"

echo "=================================="
echo " Deployment successful!"
echo "=================================="

ls -lh "$INSTALL_DIR/mediation-engine.jar"