#!/bin/bash
set -e

echo "📚 Deploying documentation to gh-pages..."

# Ensure we're on master
git checkout master
git pull origin master

# Switch to gh-pages
git checkout gh-pages
git pull origin gh-pages

# Copy docs from master
git checkout master -- docs/ README.md README.de.md

# Rename and commit
mv README.md index.md
git add index.md README.md
git commit -m "Update docs: $(date +'%Y-%m-%d %H:%M:%S')"
git push origin gh-pages

# Back to master
git checkout master

echo "✅ Documentation deployed!"
