# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

#### Integrated as a module of the Morphium multi-module project
`morphium-jakarta-data` is no longer a standalone Maven project with its own release cycle. It is now built as a module of the Morphium multi-module reactor (`morphium-parent`), lives in the `morphium-jakarta-data/` directory of the [sboesebeck/morphium](https://github.com/sboesebeck/morphium) repository, and is versioned in lockstep with Morphium core. The artifact coordinates changed from `de.caluga:morphium-jakarta-data:1.1.0` (standalone) to `de.caluga:morphium-jakarta-data:<morphium-version>` (currently `6.2.6-SNAPSHOT`). The groupId is unchanged. Existing users pinning `1.1.0`/`1.1.0-SNAPSHOT` (or the earlier `1.0.0-SNAPSHOT` line) need to bump the dependency version to match the Morphium core version they use, and should expect the artifact to be built from the Morphium reactor going forward — this repository is archived once the migration completes. No source-level API changes are part of this move; only the build/versioning model changed.

## [1.1.0-SNAPSHOT] (superseded — see [Unreleased])

This heading previously read `[Unreleased] - 1.0.0-SNAPSHOT`, which no longer reflected reality: the module had already moved past `1.0.0-SNAPSHOT` to `1.1.0-SNAPSHOT` as a standalone project before the integration into Morphium made a fixed pre-1.0 standalone version number moot altogether. The entries below are kept for history; going forward, changes are tracked under `[Unreleased]` above and, once released, under the Morphium version they ship with.

### Added
- Framework-agnostic Jakarta Data 1.0 runtime for Morphium ODM
- `AbstractMorphiumRepository` base class with full CRUD implementation
- `MorphiumRepository` extended interface (distinct, direct Morphium/Query access)
- Query derivation from method names: `findBy*`, `countBy*`, `existsBy*`, `deleteBy*`
  - Supported operators: equals, greaterThan, lessThan, like, in, between, not, and, or
- JDQL parsing via `@Query` annotation
- `@Find` / `@Delete` with `@By` parameter binding
- Pagination support: `Page<T>`, `CursoredPage<T>`, `PageRequest`
- Sorting: `Sort<T>`, `Order<T>`, `@OrderBy`
- Stream and async return types: `Stream<T>`, `CompletionStage<T>`
- `RepositoryMetadata` for entity type, ID type, and collection name resolution
