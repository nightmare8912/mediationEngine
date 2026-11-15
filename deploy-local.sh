#!/bin/bash

set -euo pipefail

# ============================================================
# 1. Directory Configuration
# ============================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

INSTALL_DIR="${MEDIATION_INSTALL_DIR:-$HOME/Desktop/MediationEngine}"
APP_DIR="${MEDIATION_APP_DIR:-/Applications/Mediation Engine.app}"


# ============================================================
# 2. Java Runtime Configuration
# ============================================================

# Build and package using Java 25, matching the services.

export JAVA_HOME="$(/usr/libexec/java_home -v 25)"
export PATH="$JAVA_HOME/bin:$PATH"


# ============================================================
# 3. Validate Installation Directory
# ============================================================

if [[ ! -d "$INSTALL_DIR" ]]; then
    echo "ERROR: Installation directory not found: $INSTALL_DIR" >&2
    exit 1
fi


# ============================================================
# 4. Build Project Artifacts
# ============================================================

cd "$PROJECT_DIR"

./gradlew clean test shadowJar desktopJar


# ============================================================
# 5. Prepare Desktop Application Packaging
# ============================================================

# Package the desktop artifact with a bundled Java 25 runtime
# so Finder launches do not depend on system Java configuration.

mkdir -p build/desktop-input build/apps

cp \
    build/libs/mediation-desktop.jar \
    build/desktop-input/

PACKAGE_DIR="$(mktemp -d "$PROJECT_DIR/build/apps/package.XXXXXX")"


# ============================================================
# 6. Generate macOS Desktop Application
# ============================================================

"$JAVA_HOME/bin/jpackage" \
    --type app-image \
    --name "Mediation Engine" \
    --app-version 1.0 \
    --vendor "Prats" \
    --input build/desktop-input \
    --main-jar mediation-desktop.jar \
    --main-class org.prats.desktop.DesktopApplication \
    --dest "$PACKAGE_DIR"


# ============================================================
# 7. Deploy Engine and Desktop JARs
# ============================================================

# Stage each JAR using a temporary filename.
#
# Renaming the staged file avoids exposing a partially copied
# JAR at the final destination.

for artifact in mediation-engine mediation-desktop; do

    SOURCE_JAR="build/libs/$artifact.jar"
    STAGED_JAR="$INSTALL_DIR/$artifact.jar.next"
    TARGET_JAR="$INSTALL_DIR/$artifact.jar"

    cp "$SOURCE_JAR" "$STAGED_JAR"

    mv -f "$STAGED_JAR" "$TARGET_JAR"

done


# ============================================================
# 8. Deploy macOS Desktop Application
# ============================================================

# Refresh the GUI application bundle, including:
#
# - Desktop JAR
# - Application launcher
# - Bundled Java runtime
# - Application metadata and signature

ditto \
    "$PACKAGE_DIR/Mediation Engine.app" \
    "$APP_DIR"


# ============================================================
# 9. Deployment Summary
# ============================================================

echo ""
echo "============================================================"
echo "             MEDIATION ENGINE DEPLOYMENT"
echo "============================================================"
echo ""
echo "Engine and GUI JARs deployed to:"
echo "  $INSTALL_DIR"
echo ""
echo "Desktop application updated:"
echo "  $APP_DIR"
echo ""
echo "Reopen the GUI to load the new build."
echo "Running services must be restarted separately."
echo ""
echo "============================================================"