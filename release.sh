#!/bin/bash
set -eo pipefail

# =============================================================================
# Morphium & PoppyDB Release Script (Multi-Module)
# =============================================================================
# This script handles the complete release process for the multi-module project:
# 1. Validates prerequisites (branch, credentials, GPG)
# 2. Runs tests (optional)
# 3. Updates version and creates release tag via maven-release-plugin
# 4. Builds release artifacts for all modules
# 5. Creates, signs & uploads 3 bundles to Maven Central:
#    - morphium-parent (POM only)
#    - morphium (jar, sources, javadoc)
#    - poppydb (jar, sources, javadoc, cli fat-jar)
# 6. Merges to master and pushes tags
#
# Usage:
#   ./release.sh [OPTIONS]
#
# Options:
#   --patch            Patch release: 6.1.9 → 6.1.10 (default)
#   --minor            Minor release: 6.1.9 → 6.2.0
#   --major            Major release: 6.1.9 → 7.0.0
#   --run-tests        Run tests before release (default: skip)
#   --dry-run          Build & bundle everything but don't upload or tag
#   --auto-publish     Automatically publish to Maven Central after validation
#   --deploy-docs      Deploy documentation to gh-pages after release
#   --rollback         Roll back the last release (renames tag, resets branches)
#   --reset            Emergency reset: clean up release leftovers, align all
#                      module versions to develop, remove dangling tags
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
BUMP_TYPE=patch
RUN_TESTS=false
DRY_RUN=false
AUTO_PUBLISH=false
DEPLOY_DOCS=false
ROLLBACK=false
RESET=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
  --patch)
    BUMP_TYPE=patch
    shift
    ;;
  --minor)
    BUMP_TYPE=minor
    shift
    ;;
  --major)
    BUMP_TYPE=major
    shift
    ;;
  --run-tests)
    RUN_TESTS=true
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
  --rollback)
    ROLLBACK=true
    shift
    ;;
  --reset)
    RESET=true
    shift
    ;;
  --help)
    sed -n '4,32p' "$0" | sed 's/^# //' | sed 's/^#//'
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

# Sign a file with GPG if not already signed
sign_file() {
  local file="$1"
  if [ -f "$file" ] && [ ! -f "${file}.asc" ]; then
    gpg --armor --detach-sign "$file" 2>/dev/null
    log_success "Signed $(basename "$file")"
  fi
}

# Generate checksums for a file
checksum_file() {
  local file="$1"
  if [ -f "$file" ]; then
    calc_md5 "$file" >"${file}.md5"
    calc_sha1 "$file" >"${file}.sha1"
  fi
}

# Upload a bundle to Sonatype Central Portal
# Usage: upload_bundle <bundle_file> <display_name>
upload_bundle() {
  local bundle_file="$1"
  local display_name="$2"

  log_info "Uploading ${display_name} bundle..."
  local response
  response=$(curl --progress-bar -w "\n%{http_code}" \
    --request POST \
    --form bundle=@"$bundle_file" \
    --form publishingType="$publishing_type" \
    --header "Authorization: Bearer $auth_token" \
    --connect-timeout 30 \
    --max-time 600 \
    https://central.sonatype.com/api/v1/publisher/upload)

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local body
  body=$(echo "$response" | sed '$d')

  if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
    log_success "${display_name} upload successful!"

    local deployment_id=""
    if echo "$body" | jq -e '.deploymentId' &>/dev/null; then
      deployment_id=$(echo "$body" | jq -r '.deploymentId')
    elif [ -n "$body" ]; then
      deployment_id="$body"
    fi

    if [ -n "$deployment_id" ]; then
      log_info "Deployment ID: $deployment_id"
    fi
  else
    log_error "${display_name} upload failed with HTTP $http_code"
    echo "$body" | jq . 2>/dev/null || echo "$body"
    return 1
  fi
}

cleanup() {
  local exit_code=$?
  if [ -n "$BUNDLE_DIR" ] && [ -d "$BUNDLE_DIR" ]; then
    rm -rf "$BUNDLE_DIR"
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
  rm -f morphium-core/pom.xml.releaseBackup poppydb/pom.xml.releaseBackup 2>/dev/null || true
  exit $exit_code
}

trap cleanup EXIT

# Record starting branch early so cleanup trap can return here on any error
ORIGINAL_BRANCH=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")

# -----------------------------------------------------------------------------
# Rollback handler
# -----------------------------------------------------------------------------

do_rollback() {
  log_step "Rolling back last release"

  # Remember where we started so we can return after rollback
  local start_branch
  start_branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")

  # Find the latest v* tag (excluding -rolled-back tags)
  local last_tag
  last_tag=$(git tag -l 'v[0-9]*' --sort=-v:refname | grep -v '\-rolled-back$' | head -n1)

  if [ -z "$last_tag" ]; then
    log_error "No release tag found to roll back"
    exit 1
  fi

  # Check if already rolled back
  if git tag -l "${last_tag}-rolled-back" | grep -q .; then
    log_error "Tag ${last_tag} was already rolled back (${last_tag}-rolled-back exists)"
    exit 1
  fi

  local tag_version="${last_tag#v}"
  log_info "Found release tag: $last_tag (version $tag_version)"

  echo ""
  echo "This will:"
  echo "  1. Rename tag $last_tag → ${last_tag}-rolled-back (local + remote)"
  echo "  2. Reset master to before the release merge"
  echo "  3. Reset develop version back to ${tag_version}-SNAPSHOT"
  echo ""

  if ! confirm "Proceed with rollback?" "n"; then
    echo "Rollback cancelled"
    exit 0
  fi

  # Step 1: Rename the tag
  log_info "Renaming tag ${last_tag} → ${last_tag}-rolled-back..."
  git tag "${last_tag}-rolled-back" "$last_tag"
  git tag -d "$last_tag"
  git push origin "${last_tag}-rolled-back" 2>/dev/null || true
  git push --delete origin "$last_tag" 2>/dev/null || true
  log_success "Tag renamed"

  # Step 2: Reset master
  log_info "Resetting master..."
  git checkout master
  git pull origin master --no-edit || true

  local tag_commit
  tag_commit=$(git rev-parse "${last_tag}-rolled-back")

  if git merge-base --is-ancestor "$tag_commit" HEAD 2>/dev/null; then
    local pre_merge
    pre_merge=$(git log --oneline --first-parent --format="%H" | while read -r sha; do
      if ! git merge-base --is-ancestor "$tag_commit" "$sha" 2>/dev/null; then
        echo "$sha"
        break
      fi
    done)

    if [ -n "$pre_merge" ]; then
      git reset --hard "$pre_merge"
      git push --force-with-lease origin master
      log_success "Master reset to before release merge"
    else
      log_warn "Could not determine pre-merge commit on master"
    fi
  else
    log_info "Master does not contain this tag - nothing to reset"
  fi

  # Step 3: Reset develop version
  log_info "Resetting develop version to ${tag_version}-SNAPSHOT..."
  git checkout develop
  git pull origin develop --no-edit || true

  mvn versions:set -DnewVersion="${tag_version}-SNAPSHOT" -DgenerateBackupPoms=false -q
  git add pom.xml morphium-core/pom.xml poppydb/pom.xml
  git commit -m "Rollback: reset version to ${tag_version}-SNAPSHOT (rolled back ${last_tag})"
  git push origin develop
  log_success "Develop version reset to ${tag_version}-SNAPSHOT"

  # Return to the branch we started on
  if [ -n "$start_branch" ] && [ "$start_branch" != "develop" ]; then
    log_info "Returning to $start_branch..."
    git checkout "$start_branch"
  fi

  log_step "Rollback complete"
  echo ""
  echo "  Rolled back: $last_tag → ${last_tag}-rolled-back"
  echo "  Master: reset to pre-release state"
  echo "  Develop: ${tag_version}-SNAPSHOT"
  echo "  Branch: $(git symbolic-ref --short HEAD)"
  echo ""
  echo "  Don't forget to delete the Sonatype deployment if it was uploaded:"
  echo "    https://central.sonatype.com/publishing/deployments"
  echo ""
  exit 0
}

if [ "$ROLLBACK" = true ]; then
  do_rollback
fi

# -----------------------------------------------------------------------------
# Reset handler — emergency cleanup for broken state
# -----------------------------------------------------------------------------

do_reset() {
  log_step "Emergency reset — cleaning up release state"

  local branch
  branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")
  log_info "Current branch: $branch"

  # 1. Clean up release leftovers
  log_info "Removing release leftovers..."
  rm -f release.properties pom.xml.releaseBackup 2>/dev/null || true
  rm -f morphium-core/pom.xml.releaseBackup poppydb/pom.xml.releaseBackup 2>/dev/null || true
  mvn release:clean -q 2>/dev/null || true
  log_success "Release leftovers cleaned"

  # 2. Detect expected version from develop branch
  local develop_version
  develop_version=$(git show develop:pom.xml 2>/dev/null | grep '<version>' | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')

  if [ -z "$develop_version" ]; then
    log_error "Cannot determine version from develop branch"
    exit 1
  fi

  log_info "Develop branch version: $develop_version"

  # 3. Check current module versions
  local parent_ver core_ver poppy_ver
  parent_ver=$(grep '<version>' pom.xml | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
  core_ver=$(grep '<version>' morphium-core/pom.xml | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
  poppy_ver=$(grep '<version>' poppydb/pom.xml | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')

  log_info "Current versions: parent=$parent_ver core=$core_ver poppydb=$poppy_ver"

  if [ "$parent_ver" != "$develop_version" ] || [ "$core_ver" != "$develop_version" ] || [ "$poppy_ver" != "$develop_version" ]; then
    log_warn "Versions are out of sync — resetting all to $develop_version"
    mvn versions:set -DnewVersion="$develop_version" -DgenerateBackupPoms=false -q
    rm -f pom.xml.versionsBackup morphium-core/pom.xml.versionsBackup poppydb/pom.xml.versionsBackup 2>/dev/null || true
    log_success "All modules set to $develop_version"
  else
    log_success "All module versions already aligned at $develop_version"
  fi

  # 4. Find and report dangling tags (released versions without matching SNAPSHOT)
  local snap_base="${develop_version%-SNAPSHOT}"
  local dangling_tag
  dangling_tag=$(git tag -l "v${snap_base}" 2>/dev/null)
  local rolled_back_tag
  rolled_back_tag=$(git tag -l "v${snap_base}-rolled-back" 2>/dev/null)

  if [ -n "$dangling_tag" ]; then
    log_warn "Found tag $dangling_tag for current SNAPSHOT version"
    if confirm "Delete tag $dangling_tag (local + remote)?"; then
      git tag -d "$dangling_tag" 2>/dev/null || true
      git push --delete origin "$dangling_tag" 2>/dev/null || true
      log_success "Deleted tag $dangling_tag"
    fi
  fi

  if [ -n "$rolled_back_tag" ]; then
    log_warn "Found rolled-back tag $rolled_back_tag"
    if confirm "Delete tag $rolled_back_tag (local + remote)?"; then
      git tag -d "$rolled_back_tag" 2>/dev/null || true
      git push --delete origin "$rolled_back_tag" 2>/dev/null || true
      log_success "Deleted tag $rolled_back_tag"
    fi
  fi

  # 5. Check for uncommitted version changes
  if ! git diff --quiet -- '*/pom.xml' pom.xml 2>/dev/null; then
    log_info "POM files were modified:"
    git diff --stat -- '*/pom.xml' pom.xml
    echo ""
    if confirm "Stage and commit the version fixes?"; then
      git add pom.xml morphium-core/pom.xml poppydb/pom.xml
      git commit -m "Reset: align all module versions to $develop_version"
      log_success "Version fix committed"
    fi
  fi

  # 6. Summary
  log_step "Reset complete"
  echo ""
  echo "  All modules: $develop_version"
  echo "  Branch: $(git symbolic-ref --short HEAD)"
  echo "  Release leftovers: cleaned"
  echo ""
  echo "  You can now try the release again."
  echo ""
  exit 0
}

if [ "$RESET" = true ]; then
  do_reset
fi

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

# Determine versions from last release tag
last_tag=$(git tag -l 'v[0-9]*' --sort=-v:refname | grep -v '\-rolled-back$' | head -n1)

if [ -z "$last_tag" ]; then
  log_error "No previous release tag found (expected v*.*.* format)"
  exit 1
fi

last_version="${last_tag#v}"
IFS='.' read -r v_major v_minor v_patch <<<"$last_version"

case "$BUMP_TYPE" in
patch) release_version="${v_major}.${v_minor}.$((v_patch + 1))" ;;
minor) release_version="${v_major}.$((v_minor + 1)).0" ;;
major) release_version="$((v_major + 1)).0.0" ;;
esac

# Next SNAPSHOT after release
IFS='.' read -r r_major r_minor r_patch <<<"$release_version"
next_snapshot="${r_major}.${r_minor}.$((r_patch + 1))-SNAPSHOT"

log_info "Last release: $last_tag"
log_info "Release version: $release_version (--${BUMP_TYPE})"
log_info "Next development: $next_snapshot"

# Align POM versions to release if needed
current_version=$(grep '<version>' pom.xml | head -n1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/')

if [ "$current_version" != "${release_version}-SNAPSHOT" ]; then

  if [ "$DRY_RUN" = true ]; then
    log_info "Not changing current version because of DRY_RUN"
  else
    log_info "POM version is $current_version, setting to ${release_version}-SNAPSHOT..."
    mvn versions:set -DnewVersion="${release_version}-SNAPSHOT" -DgenerateBackupPoms=false -q
    git add pom.xml morphium-core/pom.xml poppydb/pom.xml
    git commit -m "Set version to ${release_version}-SNAPSHOT for release" -q
    log_success "POM versions aligned to ${release_version}-SNAPSHOT"
  fi
else
  log_success "POM version: $current_version"
fi

# Verify multi-module structure
for module_dir in morphium-core poppydb; do
  if [ ! -f "$module_dir/pom.xml" ]; then
    log_error "Module directory $module_dir/pom.xml not found"
    exit 1
  fi
done
log_success "Multi-module structure: morphium-parent, morphium-core (morphium), poppydb"

# -----------------------------------------------------------------------------
# Step 2: Run tests (optional)
# -----------------------------------------------------------------------------

if [ "$RUN_TESTS" = true ]; then
  log_step "Running tests (--run-tests)"

  if ! mvn clean test -q; then
    log_error "Tests failed!"
    if ! confirm "Continue with release anyway?"; then
      exit 1
    fi
  else
    log_success "All tests passed"
  fi
else
  log_step "Skipping tests (use --run-tests to enable)"
fi

# -----------------------------------------------------------------------------
# Step 3: Dry run check
# -----------------------------------------------------------------------------

if [ "$DRY_RUN" = true ]; then
  log_step "Dry run: building and creating bundle (no tag, no upload)"

  version="$release_version"

  # Build everything
  log_info "Building all modules..."
  mvn clean package verify -DskipTests -Dmaven.javadoc.failOnError=false || {
    log_error "Package failed"
    exit 1
  }
  log_success "All modules built"

  # Create bundle (same as real release)
  BUNDLE_DIR="$(pwd)/target/bundle-staging"
  mkdir -p "$BUNDLE_DIR"

  log_info "Adding morphium-parent..."
  parent_repo="${BUNDLE_DIR}/de/caluga/morphium-parent/${version}"
  mkdir -p "$parent_repo"
  cp pom.xml "${parent_repo}/morphium-parent-${version}.pom"
  sign_file "${parent_repo}/morphium-parent-${version}.pom"
  checksum_file "${parent_repo}/morphium-parent-${version}.pom"

  log_info "Adding morphium..."
  morphium_repo="${BUNDLE_DIR}/de/caluga/morphium/${version}"
  mkdir -p "$morphium_repo"
  cp morphium-core/pom.xml "${morphium_repo}/morphium-${version}.pom"
  cp morphium-core/target/morphium-${version}-SNAPSHOT.jar "${morphium_repo}/morphium-${version}.jar" 2>/dev/null ||
    cp morphium-core/target/morphium-${version}.jar "${morphium_repo}/" 2>/dev/null || true
  cp morphium-core/target/morphium-${version}-SNAPSHOT-sources.jar "${morphium_repo}/morphium-${version}-sources.jar" 2>/dev/null ||
    cp morphium-core/target/morphium-${version}-sources.jar "${morphium_repo}/" 2>/dev/null || true
  cp morphium-core/target/morphium-${version}-SNAPSHOT-javadoc.jar "${morphium_repo}/morphium-${version}-javadoc.jar" 2>/dev/null ||
    cp morphium-core/target/morphium-${version}-javadoc.jar "${morphium_repo}/" 2>/dev/null || true

  log_info "Adding poppydb..."
  poppydb_repo="${BUNDLE_DIR}/de/caluga/poppydb/${version}"
  mkdir -p "$poppydb_repo"
  cp poppydb/pom.xml "${poppydb_repo}/poppydb-${version}.pom"
  cp poppydb/target/poppydb-${version}-SNAPSHOT.jar "${poppydb_repo}/poppydb-${version}.jar" 2>/dev/null ||
    cp poppydb/target/poppydb-${version}.jar "${poppydb_repo}/" 2>/dev/null || true
  cp poppydb/target/poppydb-${version}-SNAPSHOT-sources.jar "${poppydb_repo}/poppydb-${version}-sources.jar" 2>/dev/null ||
    cp poppydb/target/poppydb-${version}-sources.jar "${poppydb_repo}/" 2>/dev/null || true
  cp poppydb/target/poppydb-${version}-SNAPSHOT-javadoc.jar "${poppydb_repo}/poppydb-${version}-javadoc.jar" 2>/dev/null ||
    cp poppydb/target/poppydb-${version}-javadoc.jar "${poppydb_repo}/" 2>/dev/null || true
  cp poppydb/target/poppydb-${version}-SNAPSHOT-cli.jar "${poppydb_repo}/poppydb-${version}-cli.jar" 2>/dev/null ||
    cp poppydb/target/poppydb-${version}-cli.jar "${poppydb_repo}/" 2>/dev/null || true

  bundle_file="target/bundle-${version}.jar"
  (cd "$BUNDLE_DIR" && zip -q -r "$(pwd)/../bundle-${version}.jar" de/)

  log_step "Dry run complete"
  echo ""
  echo "Would release version: $release_version"
  echo "  Modules: morphium-parent, morphium, poppydb"
  echo "  From branch: $branch"
  echo ""
  echo "Bundle contents:"
  (cd "$BUNDLE_DIR" && find de/ -type f | sort)
  echo ""
  echo "Bundle size: $(du -h "$bundle_file" | cut -f1)"
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
echo "  Last release: $last_tag"
echo "  Release version: $release_version (--${BUMP_TYPE})"
echo "  Next development: $next_snapshot"
echo "  Modules: morphium-parent, morphium, poppydb"
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

# Clean up rolled-back tag from previous attempt if present
rolled_back_tag="v${release_version}-rolled-back"
if git tag -l "$rolled_back_tag" | grep -q .; then
  log_info "Cleaning up rolled-back tag: $rolled_back_tag"
  git tag -d "$rolled_back_tag" 2>/dev/null || true
  git push --delete origin "$rolled_back_tag" 2>/dev/null || true
  log_success "Removed $rolled_back_tag"
fi

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
log_info "  Release: $release_version → Next: $next_snapshot"
mvn release:clean release:prepare \
  -DreleaseVersion="$release_version" \
  -DdevelopmentVersion="$next_snapshot" \
  -Dtag="v${release_version}" \
  2>&1 | tee -a "$RELEASE_LOG"

# Extract version and tag from release.properties
if [ ! -f release.properties ]; then
  log_error "release.properties not found after release:prepare"
  exit 1
fi

# Multi-module: parent artifactId is morphium-parent
version=$(grep "project.rel.de.caluga\\\\:morphium-parent" release.properties | cut -f2 -d= || echo "$release_version")
tag=$(grep "scm.tag=" release.properties | cut -f2 -d=)

log_success "Release prepared: $version (tag: $tag)"

# -----------------------------------------------------------------------------
# Step 6: Build artifacts
# -----------------------------------------------------------------------------

log_step "Building release artifacts"

# Checkout the release tag to build the correct version
log_info "Checking out release tag $tag..."
git checkout "$tag"

# Build all modules
log_info "Building all modules for version $version..."
mvn clean package verify -DskipTests -Dmaven.javadoc.failOnError=false || {
  log_error "Package failed"
  exit 1
}

log_success "All modules built"

# -----------------------------------------------------------------------------
# Step 7: Create combined bundle (parent + morphium + poppydb)
# -----------------------------------------------------------------------------

log_step "Creating combined release bundle"

BUNDLE_DIR="$(pwd)/target/bundle-staging"
mkdir -p "$BUNDLE_DIR"

# --- morphium-parent (POM-only) ---
log_info "Adding morphium-parent..."
parent_repo="${BUNDLE_DIR}/de/caluga/morphium-parent/${version}"
mkdir -p "$parent_repo"

cp pom.xml "${parent_repo}/morphium-parent-${version}.pom"
sign_file "${parent_repo}/morphium-parent-${version}.pom"
checksum_file "${parent_repo}/morphium-parent-${version}.pom"

# --- morphium (morphium-core module, artifactId=morphium) ---
log_info "Adding morphium..."
morphium_repo="${BUNDLE_DIR}/de/caluga/morphium/${version}"
mkdir -p "$morphium_repo"

cp morphium-core/pom.xml "${morphium_repo}/morphium-${version}.pom"
cp morphium-core/target/morphium-${version}.jar "${morphium_repo}/"
cp morphium-core/target/morphium-${version}-sources.jar "${morphium_repo}/"
cp morphium-core/target/morphium-${version}-javadoc.jar "${morphium_repo}/"

for file in "${morphium_repo}"/morphium-${version}*; do
  [ -f "$file" ] || continue
  sign_file "$file"
  checksum_file "$file"
done

# --- poppydb ---
log_info "Adding poppydb..."
poppydb_repo="${BUNDLE_DIR}/de/caluga/poppydb/${version}"
mkdir -p "$poppydb_repo"

cp poppydb/pom.xml "${poppydb_repo}/poppydb-${version}.pom"
cp poppydb/target/poppydb-${version}.jar "${poppydb_repo}/"
cp poppydb/target/poppydb-${version}-sources.jar "${poppydb_repo}/"
cp poppydb/target/poppydb-${version}-javadoc.jar "${poppydb_repo}/"
if [ -f "poppydb/target/poppydb-${version}-cli.jar" ]; then
  cp "poppydb/target/poppydb-${version}-cli.jar" "${poppydb_repo}/"
fi

for file in "${poppydb_repo}"/poppydb-${version}*; do
  [ -f "$file" ] || continue
  sign_file "$file"
  checksum_file "$file"
done

# Verify all required files
log_info "Verifying artifacts..."
for suffix in .pom .pom.asc .jar .jar.asc -sources.jar -sources.jar.asc -javadoc.jar -javadoc.jar.asc; do
  for artifact_repo in "$morphium_repo/morphium" "$poppydb_repo/poppydb"; do
    if [ ! -f "${artifact_repo}-${version}${suffix}" ]; then
      log_error "Missing: $(basename "${artifact_repo}-${version}${suffix}")"
      exit 1
    fi
  done
done
log_success "All required artifacts present"

# Create single combined bundle
bundle_file="target/bundle-${version}.jar"
(cd "$BUNDLE_DIR" && zip -q -r "$(pwd)/../bundle-${version}.jar" de/)

log_success "Combined bundle: $bundle_file ($(du -h "$bundle_file" | cut -f1))"
log_info "  Contents: morphium-parent (pom), morphium (jar+sources+javadoc), poppydb (jar+sources+javadoc+cli)"

# -----------------------------------------------------------------------------
# Step 8: Upload to Sonatype Central Portal
# -----------------------------------------------------------------------------

log_step "Uploading to Sonatype Central Portal"

if [ "$AUTO_PUBLISH" = true ]; then
  publishing_type="AUTOMATIC"
  log_info "Publishing mode: AUTOMATIC (will publish immediately after validation)"
else
  publishing_type="USER_MANAGED"
  log_info "Publishing mode: USER_MANAGED (requires manual publish via Portal UI)"
fi

# Create base64 encoded credentials
auth_token=$(echo -n "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" | base64)

upload_bundle "$bundle_file" "morphium+poppydb" || exit 1

log_success "Bundle uploaded"
log_info "Monitor at: https://central.sonatype.com/publishing/deployments"

# -----------------------------------------------------------------------------
# Step 9: Git operations - merge to master and push
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

# Push develop (release:prepare already committed the next SNAPSHOT there)
git checkout develop
git push origin develop || true

# Return to original branch
if [ -n "$ORIGINAL_BRANCH" ] && [ "$ORIGINAL_BRANCH" != "develop" ]; then
  git checkout "$ORIGINAL_BRANCH"
fi

# Clear ORIGINAL_BRANCH so cleanup trap doesn't try to switch again
ORIGINAL_BRANCH=""

log_success "Back on $branch branch"

# Clean up release leftovers (also in trap, but be thorough)
rm -f release.properties pom.xml.releaseBackup 2>/dev/null || true
rm -f morphium-core/pom.xml.releaseBackup poppydb/pom.xml.releaseBackup 2>/dev/null || true

# -----------------------------------------------------------------------------
# Step 10: Deploy documentation (optional)
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
# Step 11: Summary
# -----------------------------------------------------------------------------

log_step "Release complete!"

echo ""
echo "=============================================="
echo "  Morphium + PoppyDB $version released!"
echo "=============================================="
echo ""
echo "  Git tag: $tag"
echo "  Release log: $RELEASE_LOG"
echo ""
echo "  Bundle: $bundle_file"
echo "    morphium-parent (POM)"
echo "    morphium (jar, sources, javadoc)"
echo "    poppydb (jar, sources, javadoc, cli)"
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
echo "    https://repo1.maven.org/maven2/de/caluga/poppydb/$version/"
echo ""
