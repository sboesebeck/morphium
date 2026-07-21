# Contributing to Documentation

This guide explains how to contribute to and work with the Morphium documentation.

## Viewing the Documentation

### Online
Visit the published documentation at: **https://sboesebeck.github.io/morphium/**

### Local Development

1. **Install MkDocs and Material theme:**
   ```bash
   pip install mkdocs-material
   ```

2. **Serve locally:**
   ```bash
   mkdocs serve
   ```
   Then open http://127.0.0.1:8000

3. **Build static site:**
   ```bash
   mkdocs build
   ```
   Output will be in the `site/` directory.

## Documentation Structure

```
docs/
├── index.md                          # Home page
├── overview.md                       # Getting started overview
├── architecture-overview.md          # Architecture details
├── developer-guide.md                # Developer guide
├── messaging.md                      # Messaging system
├── api-reference.md                  # API reference
├── configuration-reference.md        # Configuration options
├── performance-scalability-guide.md  # Performance tuning
├── production-deployment-guide.md    # Production deployment
├── monitoring-metrics-guide.md       # Monitoring and metrics
├── security-guide.md                 # Security best practices
├── troubleshooting-guide.md          # Troubleshooting
└── howtos/                           # How-to guides
    ├── basic-setup.md
    ├── migration-v5-to-v6.md
    ├── inmemory-driver.md
    ├── aggregation-examples.md
    ├── caching-examples.md
    ├── cache-patterns.md
    ├── field-names.md
    └── messaging-implementations.md
```

## Automated Deployment

The documentation is automatically deployed to GitHub Pages when changes are pushed to the `master` or `develop` branch.

### GitHub Actions Workflow
- File: `.github/workflows/deploy-docs.yml`
- Triggers on: Push to `master`/`develop` with changes to `docs/**` or `mkdocs.yml`
- Manual trigger: Available via GitHub Actions UI

### How it works:
1. Checks out the repository
2. Installs Python and MkDocs dependencies
3. Builds the documentation site
4. Deploys to the `gh-pages` branch
5. GitHub Pages serves the content

## Navigation Features

The Material theme provides:
- **Tabbed Navigation**: Top-level categories
- **Sidebar Navigation**: Nested page structure
- **Search**: Full-text search across all docs
- **Dark/Light Mode**: Theme switcher
- **Code Highlighting**: Syntax highlighting for code blocks
- **Mobile Responsive**: Works on all devices
- **Table of Contents**: Per-page TOC on the right

## Editing Documentation

1. Edit Markdown files in the `docs/` directory
2. Add new pages to `mkdocs.yml` navigation
3. Test locally with `mkdocs serve`
4. Commit and push to trigger automatic deployment

### Markdown Extensions

Supported features:
- **Admonitions**: `!!! note`, `!!! warning`, etc.
- **Code Blocks**: Triple backticks with syntax highlighting
- **Tables**: Standard Markdown tables
- **Tabbed Content**: Using pymdownx.tabbed
- **Task Lists**: `- [ ]` and `- [x]`

## Theme Configuration

The Material theme is configured in `mkdocs.yml`:
- **Colors**: Indigo primary/accent
- **Features**: Navigation tabs, expand sections, search suggestions
- **Plugins**: Search, tags
- **Social**: GitHub repository link

## Troubleshooting

**Issue**: Theme not found
```bash
pip install mkdocs-material
```

**Issue**: Build fails
```bash
mkdocs build --strict  # Shows detailed errors
```

**Issue**: Navigation not updating
- Check `mkdocs.yml` nav structure
- Ensure file paths are correct relative to `docs/`

## Contributing

When adding new documentation:
1. Place in appropriate directory (`docs/` or `docs/howtos/`)
2. Update `mkdocs.yml` navigation
3. Use consistent heading levels (H1 for title, H2 for sections)
4. Add code examples where applicable
5. Test locally before committing
