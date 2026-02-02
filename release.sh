#!/bin/bash
set -eo pipefail

# =============================================================================
# Morphium Release Script
# =============================================================================
# This script handles the complete release process:
# 1. Validates prerequisites (branch, credentials, GPG)
# 2. Runs tests (optional)
# 3. Updates version and creates release tag via maven-release-plugin
# 4. Builds release artifacts (jar, sources, javadoc, server-cli)
# 5. Signs artifacts with GPG
# 6. Creates Maven Central bundle
# 7. Uploads to Sonatype Central Portal
# 8. Merges to master and pushes tags
#
# Usage:
#   ./release.sh [OPTIONS]
#
# Options:
#   --skip-tests       Skip running tests before release
#   --dry-run          Validate everything but don't actually release
#   --auto-publish     Automatically publish to Maven Central after validation
#   --deploy-docs      Deploy documentation to gh-pages after release
#   --help             Show this help message
#
# Prerequisites:
#   - SONATYPE_USERNAME and SONATYPE_PASSWORD environment variables
#   - GPG key configured for signing
#   - On develop branch with clean working directory
# =============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
SKIP_TESTS=false
DRY_RUN=false
AUTO_PUBLISH=false
DEPLOY_DOCS=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
	case $1 in
	--skip-tests)
		SKIP_TESTS=true
		shift
		;;
	--dry-run)
		DRY_RUN=true
		shift
		;;
	--auto-publish)
		AUTO_PUBLISH=true
		shift
		;;
	--deploy-docs)
		DEPLOY_DOCS=true
		shift
		;;
	--help)
		sed -n '4,30p' "$0" | sed 's/^# //' | sed 's/^#//'
		exit 0
		;;
	*)
		echo -e "${RED}Unknown option: $1${NC}"
		echo "Use --help for usage information"
		exit 1
		;;
	esac
done

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

log_step() {
	echo ""
	echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}"
	echo ""
}

log_info() {
	echo -e "${BLUE}   $1${NC}"
}

log_warn() {
	echo -e "${YELLOW}⚠  $1${NC}"
}

log_error() {
	echo -e "${RED}✗  $1${NC}"
}

log_success() {
	echo -e "${GREEN}✓  $1${NC}"
}

confirm() {
	local prompt="$1"
	local default="${2:-n}"

	if [[ "$default" == "y" ]]; then
		prompt="$prompt [Y/n] "
	else
		prompt="$prompt [y/N] "
	fi

	read -r -p "$prompt" response
	response=${response:-$default}

	[[ "$response" =~ ^[Yy]$ ]]
}

cleanup() {
	local exit_code=$?
	if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
		rm -rf "$TEMP_DIR"
	fi
	# Always return to the original branch on exit
	if [ -n "$ORIGINAL_BRANCH" ]; then
		current=$(git symbolic-ref --short HEAD 2>/dev/null || echo "detached")
		if [ "$current" != "$ORIGINAL_BRANCH" ]; then
			echo -e "${YELLOW}⚠  Returning to $ORIGINAL_BRANCH branch${NC}"
			git checkout "$ORIGINAL_BRANCH" 2>/dev/null || true
		fi
	fi
	# Clean up release leftovers
	rm -f release.properties pom.xml.releaseBackup 2>/dev/null || true
	exit $exit_code
}

trap cleanup EXIT

# -----------------------------------------------------------------------------
# Step 1: Validate prerequisites
# -----------------------------------------------------------------------------

log_step "Validating prerequisites"

# Check branch
branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")

if [ -z "$branch" ]; then
	log_error "Not in a git repository or detached HEAD"
	exit 1
fi

if [ "$branch" == "master" ]; then
	log_error "Cannot release from master branch. Please use develop branch."
	exit 1
fi

if [ "$branch" != "develop" ]; then
	log_warn "You are on branch '$branch', not 'develop'"
	if ! confirm "Continue anyway?"; then
		exit 1
	fi
fi
log_success "Branch: $branch"
ORIGINAL_BRANCH="$branch"

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
	log_error "Working directory has uncommitted changes"
	echo "Please commit or stash your changes before releasing"
	git status --short
	exit 1
fi
log_success "Working directory is clean"

# Check credentials
if [ -z "$SONATYPE_USERNAME" ] || [ -z "$SONATYPE_PASSWORD" ]; then
	log_error "SONATYPE_USERNAME and SONATYPE_PASSWORD environment variables must be set"
	echo ""
	echo "Set them using:"
	echo "  export SONATYPE_USERNAME='your-username'"
	echo "  export SONATYPE_PASSWORD='your-password'"
	echo ""
	echo "Get credentials from: https://central.sonatype.com/account"
	exit 1
fi
log_success "Sonatype credentials configured"

# Check GPG
if ! command -v gpg &>/dev/null; then
	log_error "GPG is not installed"
	exit 1
fi

if ! gpg --list-secret-keys --keyid-format LONG 2>/dev/null | grep -q sec; then
	log_error "No GPG secret key found"
	echo "Please configure a GPG key for signing artifacts"
	exit 1
fi
log_success "GPG key available"

# Check Java version
java_version=$(java -version 2>&1 | head -n1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$java_version" -lt 21 ]; then
	log_warn "Java 21+ recommended, found: $java_version"
fi
log_success "Java version: $(java -version 2>&1 | head -n1)"

# Get current version
current_version=$(grep '<version>' pom.xml | head -n1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
log_info "Current version: $current_version"

if [[ ! "$current_version" == *"-SNAPSHOT"* ]]; then
	log_error "Current version ($current_version) is not a SNAPSHOT version"
	echo "Release process expects a SNAPSHOT version to release"
	exit 1
fi

release_version="${current_version%-SNAPSHOT}"
log_info "Release version will be: $release_version"

# -----------------------------------------------------------------------------
# Step 2: Run tests (optional)
# -----------------------------------------------------------------------------

if [ "$SKIP_TESTS" = true ]; then
	log_step "Skipping tests (--skip-tests)"
else
	log_step "Running tests"

	if ! mvn clean test -q; then
		log_error "Tests failed!"
		if ! confirm "Continue with release anyway?"; then
			exit 1
		fi
	else
		log_success "All tests passed"
	fi
fi

# -----------------------------------------------------------------------------
# Step 3: Dry run check
# -----------------------------------------------------------------------------

if [ "$DRY_RUN" = true ]; then
	log_step "Dry run complete"
	echo ""
	echo "Would release version: $release_version"
	echo "From branch: $branch"
	echo "Auto-publish: $AUTO_PUBLISH"
	echo ""
	echo "Run without --dry-run to perform actual release"
	exit 0
fi

# -----------------------------------------------------------------------------
# Step 4: Confirm release
# -----------------------------------------------------------------------------

log_step "Release confirmation"
echo ""
echo "About to release:"
echo "  Version: $release_version"
echo "  Branch: $branch"
echo "  Auto-publish: $AUTO_PUBLISH"
echo ""

if ! confirm "Proceed with release?" "n"; then
	echo "Release cancelled"
	exit 0
fi

# -----------------------------------------------------------------------------
# Step 5: Maven release:prepare (tag + version bump)
# -----------------------------------------------------------------------------

log_step "Preparing release with Maven"

# Create log directory
mkdir -p logs
RELEASE_LOG="logs/release-${release_version}-$(date +%Y%m%d-%H%M%S).log"

log_info "Release log: $RELEASE_LOG"

# Clean and compile first
mvn clean compile -q || {
	log_error "Compile failed"
	exit 1
}

# Run release:prepare — creates tag, bumps to next SNAPSHOT, pushes both commits
log_info "Running mvn release:prepare..."
mvn release:clean release:prepare 2>&1 | tee -a "$RELEASE_LOG"

# Extract version and tag from release.properties
if [ ! -f release.properties ]; then
	log_error "release.properties not found after release:prepare"
	exit 1
fi

version=$(grep "project.rel.de.caluga\\\\:morphium" release.properties | cut -f2 -d= || echo "$release_version")
tag=$(grep "scm.tag=" release.properties | cut -f2 -d=)

log_success "Release prepared: $version (tag: $tag)"

# -----------------------------------------------------------------------------
# Step 6: Create and sign bundle
# -----------------------------------------------------------------------------

log_step "Creating release bundle"

# Checkout the release tag to build the correct version
log_info "Checking out release tag $tag..."
git checkout "$tag"

# Build with all artifacts (skip javadoc errors - they shouldn't block release)
log_info "Building artifacts for version $version..."
mvn clean package verify -DskipTests -Dmaven.javadoc.failOnError=false || {
	log_error "Package failed"
	exit 1
}

cd target

# Copy pom
cp ../pom.xml "morphium-${version}.pom" 2>/dev/null || true

# Sign artifacts
log_info "Signing artifacts..."
for file in morphium-${version}.pom morphium-${version}.jar morphium-${version}-sources.jar morphium-${version}-javadoc.jar morphium-${version}-server-cli.jar; do
	if [ -f "$file" ] && [ ! -f "${file}.asc" ]; then
		gpg --armor --detach-sign "$file" 2>/dev/null
		log_success "Signed $file"
	fi
done

# Verify required files
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

log_info "Verifying required files..."
for file in "${required_files[@]}"; do
	if [ ! -f "$file" ]; then
		log_error "Missing required file: $file"
		exit 1
	fi
done
log_success "All required files present"

# Generate checksums (macOS compatible)
log_info "Generating checksums..."

# Cross-platform checksum helpers
calc_md5() {
	if command -v md5sum &>/dev/null; then
		md5sum "$1" | awk '{print $1}'
	elif command -v md5 &>/dev/null; then
		md5 -q "$1"
	else
		openssl dgst -md5 "$1" | awk '{print $2}'
	fi
}

calc_sha1() {
	if command -v sha1sum &>/dev/null; then
		sha1sum "$1" | awk '{print $1}'
	elif command -v shasum &>/dev/null; then
		shasum "$1" | awk '{print $1}'
	else
		openssl dgst -sha1 "$1" | awk '{print $2}'
	fi
}

for file in morphium-${version}.pom morphium-${version}.jar morphium-${version}-sources.jar morphium-${version}-javadoc.jar; do
	if [ -f "$file" ]; then
		calc_md5 "$file" >"${file}.md5"
		calc_sha1 "$file" >"${file}.sha1"
	fi
done

# Also handle server-cli if present
if [ -f "morphium-${version}-server-cli.jar" ]; then
	calc_md5 "morphium-${version}-server-cli.jar" >"morphium-${version}-server-cli.jar.md5"
	calc_sha1 "morphium-${version}-server-cli.jar" >"morphium-${version}-server-cli.jar.sha1"
fi

# Create Maven repository structure
log_info "Creating Maven repository structure..."
repo_path="de/caluga/morphium/${version}"
mkdir -p "$repo_path"

for file in morphium-${version}*; do
	cp "$file" "$repo_path/"
done

# Create bundle
bundle_file="bundle_${version}.jar"
zip -q -r "$bundle_file" de/

log_success "Bundle created: $bundle_file ($(du -h "$bundle_file" | cut -f1))"

cd ..

# -----------------------------------------------------------------------------
# Step 7: Upload to Sonatype Central Portal
# -----------------------------------------------------------------------------

log_step "Uploading to Sonatype Central Portal"

cd target

if [ "$AUTO_PUBLISH" = true ]; then
	publishing_type="AUTOMATIC"
	log_info "Publishing mode: AUTOMATIC (will publish immediately after validation)"
else
	publishing_type="USER_MANAGED"
	log_info "Publishing mode: USER_MANAGED (requires manual publish via Portal UI)"
fi

# Create base64 encoded credentials
auth_token=$(echo -n "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" | base64)

# Upload bundle
log_info "Uploading bundle..."
response=$(curl --progress-bar -w "\n%{http_code}" \
	--request POST \
	--form bundle=@"$bundle_file" \
	--form publishingType="$publishing_type" \
	--header "Authorization: Bearer $auth_token" \
	--connect-timeout 30 \
	--max-time 600 \
	https://central.sonatype.com/api/v1/publisher/upload)

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
	log_success "Upload successful!"

	# Sonatype Central Portal returns deployment ID as plain text or JSON
	deployment_id=""
	if echo "$body" | jq -e '.deploymentId' &>/dev/null; then
		deployment_id=$(echo "$body" | jq -r '.deploymentId')
	elif [ -n "$body" ]; then
		# Plain text response — the body IS the deployment ID
		deployment_id="$body"
	fi

	if [ -n "$deployment_id" ]; then
		log_info "Deployment ID: $deployment_id"
		log_info "Monitor at: https://central.sonatype.com/publishing/deployments"
	fi
else
	log_error "Upload failed with HTTP $http_code"
	echo "$body" | jq . 2>/dev/null || echo "$body"
	exit 1
fi

cd ..

# -----------------------------------------------------------------------------
# Step 8: Git operations - merge to master and push
# -----------------------------------------------------------------------------

log_step "Finalizing git operations"

# We're currently on the tag (detached HEAD), need to go back to branches
log_info "Pushing tags..."
git push --tags
log_success "Tags pushed"

log_info "Merging $tag to master..."
git fetch origin
git checkout master
git pull origin master --no-edit || true
git merge "$tag" --no-edit || {
	log_warn "Merge to master failed (maybe already up to date)"
}
git push origin master || {
	log_warn "Push to master failed"
}

log_success "Merged to master"

# Return to develop and push
git checkout develop
git push origin develop || true

# Clear ORIGINAL_BRANCH so cleanup trap doesn't try to switch again
ORIGINAL_BRANCH=""

log_success "Back on develop branch"

# Clean up release leftovers (also in trap, but be thorough)
rm -f release.properties pom.xml.releaseBackup 2>/dev/null || true

# -----------------------------------------------------------------------------
# Step 9: Deploy documentation (optional)
# -----------------------------------------------------------------------------

if [ "$DEPLOY_DOCS" = true ]; then
	log_step "Deploying documentation"

	if [ -f "./deploy_docs.sh" ]; then
		./deploy_docs.sh
		log_success "Documentation deployed to gh-pages"
	else
		log_warn "deploy_docs.sh not found, skipping documentation deployment"
	fi
else
	log_info "Skipping documentation deployment (use --deploy-docs to enable)"
fi

# -----------------------------------------------------------------------------
# Step 10: Summary
# -----------------------------------------------------------------------------

log_step "Release complete!"

echo ""
echo "=============================================="
echo "  Morphium $version released successfully!"
echo "=============================================="
echo ""
echo "  Git tag: $tag"
echo "  Bundle: target/$bundle_file"
echo "  Release log: $RELEASE_LOG"
echo ""

if [ "$AUTO_PUBLISH" = true ]; then
	echo "  Maven Central: Auto-publishing (10-30 min validation)"
else
	echo "  Maven Central: Manual publish required"
	echo "    1. Go to https://central.sonatype.com/publishing/deployments"
	echo "    2. Find deployment and click 'Publish'"
fi

echo ""
echo "  After publish, artifacts will be available at:"
echo "    https://repo1.maven.org/maven2/de/caluga/morphium/$version/"
echo ""
