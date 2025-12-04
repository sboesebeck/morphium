#!/bin/bash
set -e

echo "Building and packaging..."
mvn clean package verify -DskipTests || exit 1

echo "Extracting version..."
version=$(grep '<version>' pom.xml | head -n1 | tr -d ' a-z<>/')
echo "Version: $version"

# Check if this is a SNAPSHOT version
if [[ $version == *"SNAPSHOT"* ]]; then
    echo "ERROR: Cannot deploy SNAPSHOT versions to Maven Central"
    echo "Please set a release version in pom.xml"
    exit 1
fi

cd target

# Verify all required files exist
required_files=(
    "morphium-${version}.pom"
    "morphium-${version}.pom.asc"
    "morphium-${version}.jar"
    "morphium-${version}.jar.asc"
    "morphium-${version}-sources.jar"
    "morphium-${version}-sources.jar.asc"
    "morphium-${version}-javadoc.jar"
    "morphium-${version}-javadoc.jar.asc"
    "morphium-${version}-server-cli.jar"
    "morphium-${version}-server-cli.jar.asc"
)

echo "Verifying required files..."
for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo "ERROR: Missing required file: $file"
        exit 1
    fi
    echo "  ✓ $file"
done

# Verify GPG signatures
echo "Verifying GPG signatures..."
for asc_file in *.asc; do
    base_file="${asc_file%.asc}"
    if ! gpg --verify "$asc_file" "$base_file" 2>/dev/null; then
        echo "WARNING: GPG signature verification failed for $asc_file"
        echo "This may cause rejection by Maven Central"
    else
        echo "  ✓ $asc_file verified"
    fi
done

echo "Generating MD5 and SHA1 checksums..."
for file in morphium-${version}.pom morphium-${version}.jar morphium-${version}-sources.jar morphium-${version}-javadoc.jar morphium-${version}-server-cli.jar; do
    if [ -f "$file" ]; then
        md5sum "$file" | awk '{print $1}' > "${file}.md5"
        sha1sum "$file" | awk '{print $1}' > "${file}.sha1"
        echo "  ✓ ${file}.md5"
        echo "  ✓ ${file}.sha1"
    fi
done

echo "Creating Maven repository structure..."
# Maven Central expects: de/caluga/morphium/VERSION/files
repo_path="de/caluga/morphium/${version}"
mkdir -p "$repo_path"

# Copy all artifacts to proper location
for file in morphium-${version}*; do
    cp "$file" "$repo_path/"
done

echo "Creating bundle..."
# Create bundle with proper Maven directory structure (stay in target/ directory)
zip -q -r bundle_${version}.jar de/

echo ""
echo "Bundle created successfully: target/bundle_${version}.jar"
echo ""
echo "Bundle contents:"
unzip -l bundle_${version}.jar | grep "morphium-"
echo ""
echo "To upload to Maven Central:"
echo "  1. Go to https://central.sonatype.com/publishing"
echo "  2. Click 'Upload Bundle'"
echo "  3. Select target/bundle_${version}.jar"
echo "  4. Wait for validation (usually 10-30 minutes)"
echo ""

