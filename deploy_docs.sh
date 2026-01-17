#!/bin/bash
set -e

# =============================================================================
# Morphium Documentation Deployment Script
# =============================================================================
# Deploys documentation to gh-pages branch for GitHub Pages hosting.
#
# Usage:
#   ./deploy_docs.sh
#
# This script:
#   1. Checks out master branch
#   2. Switches to gh-pages branch
#   3. Copies docs/, README.md, README.de.md from master
#   4. Creates index.md from README.md for GitHub Pages
#   5. Commits and pushes to gh-pages
#   6. Returns to the original branch
# =============================================================================

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}📚 Deploying documentation to gh-pages...${NC}"

# Remember current branch
original_branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")

# Ensure working directory is clean
if ! git diff-index --quiet HEAD --; then
	echo "ERROR: Working directory has uncommitted changes"
	echo "Please commit or stash your changes before deploying docs"
	exit 1
fi

# Ensure we have latest master
echo "Fetching latest from master..."
git fetch origin master

# Switch to gh-pages
echo "Switching to gh-pages branch..."
git checkout gh-pages
git pull origin gh-pages

# Copy docs from master
echo "Copying documentation from master..."
git checkout origin/master -- docs/ README.md README.de.md

# Create index.md from README.md for GitHub Pages
cp README.md index.md

# Stage changes
git add docs/ README.md README.de.md index.md

# Check if there are changes to commit
if git diff --cached --quiet; then
	echo "No documentation changes to deploy"
else
	# Commit and push
	git commit -m "Update docs: $(date +'%Y-%m-%d %H:%M:%S')"
	git push origin gh-pages
	echo -e "${GREEN}✅ Documentation deployed!${NC}"
fi

# Return to original branch
if [ -n "$original_branch" ]; then
	echo "Returning to $original_branch branch..."
	git checkout "$original_branch"
else
	git checkout master
fi

echo -e "${GREEN}Done!${NC}"
