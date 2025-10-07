#!/bin/bash
set -e

# Sonatype Central Portal API Publishing Script
# Requires: SONATYPE_USERNAME and SONATYPE_PASSWORD environment variables

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

# Check for credentials
if [ -z "$SONATYPE_USERNAME" ] || [ -z "$SONATYPE_PASSWORD" ]; then
    echo "ERROR: SONATYPE_USERNAME and SONATYPE_PASSWORD environment variables must be set"
    echo ""
    echo "Usage:"
    echo "  export SONATYPE_USERNAME='your-username'"
    echo "  export SONATYPE_PASSWORD='your-password'"
    echo "  ./publish_to_central.sh [--auto-publish]"
    echo ""
    echo "Or use a token from https://central.sonatype.com/account"
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
for asc_file in morphium-${version}*.asc; do
    base_file="${asc_file%.asc}"
    if ! gpg --verify "$asc_file" "$base_file" 2>/dev/null; then
        echo "WARNING: GPG signature verification failed for $asc_file"
        echo "This may cause rejection by Maven Central"
    else
        echo "  ✓ $asc_file verified"
    fi
done

echo "Generating MD5 and SHA1 checksums..."
for file in morphium-${version}.pom morphium-${version}.jar morphium-${version}-sources.jar morphium-${version}-javadoc.jar; do
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
bundle_file="bundle_${version}.jar"
zip -q -r "$bundle_file" de/

echo ""
echo "Bundle created successfully: target/$bundle_file"
echo "Bundle size: $(du -h "$bundle_file" | cut -f1)"
echo ""

# Determine publishing type
if [ "$1" == "--auto-publish" ]; then
    publishing_type="AUTOMATIC"
    echo "Publishing mode: AUTOMATIC (will publish immediately after validation)"
else
    publishing_type="USER_MANAGED"
    echo "Publishing mode: USER_MANAGED (requires manual publish via Portal UI)"
    echo "Use --auto-publish flag to automatically publish after validation"
fi

echo ""
echo "Uploading to Sonatype Central Portal..."

# Create base64 encoded credentials
auth_token=$(echo -n "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" | base64)

# Upload bundle via API
response=$(curl -s -w "\n%{http_code}" \
    --request POST \
    --form bundle=@"$bundle_file" \
    --form publishingType="$publishing_type" \
    --header "Authorization: Bearer $auth_token" \
    https://central.sonatype.com/api/v1/publisher/upload)

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)

if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
    echo "✓ Upload successful!"
    echo ""
    echo "Response:"
    echo "$body" | jq . 2>/dev/null || echo "$body"

    # Try to extract deployment ID
    deployment_id=$(echo "$body" | jq -r '.deploymentId // empty' 2>/dev/null)

    if [ -n "$deployment_id" ]; then
        echo ""
        echo "Deployment ID: $deployment_id"
        echo ""
        echo "Monitor status at: https://central.sonatype.com/publishing/deployments"

        if [ "$publishing_type" == "USER_MANAGED" ]; then
            echo ""
            echo "After validation completes:"
            echo "  1. Go to https://central.sonatype.com/publishing/deployments"
            echo "  2. Find deployment: $deployment_id"
            echo "  3. Click 'Publish' to release to Maven Central"
        else
            echo ""
            echo "Deployment will be automatically published after validation (10-30 minutes)"
        fi
    fi

    echo ""
    echo "Validation typically takes 10-30 minutes."
    echo "Once published, it may take up to 2 hours to appear on Maven Central."
else
    echo "✗ Upload failed with HTTP $http_code"
    echo ""
    echo "Error response:"
    echo "$body" | jq . 2>/dev/null || echo "$body"
    exit 1
fi

echo ""
echo "Done!"
