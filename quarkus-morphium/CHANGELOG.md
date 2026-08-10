# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

#### Integrated into the Morphium reactor as an optional module
`quarkus-morphium` moves from a standalone repository under the `io.quarkiverse.morphium`
groupId into `sboesebeck/morphium` as an optional, four-submodule (`runtime`,
`deployment`, `testing`, `integration-tests`) reactor module under the `de.caluga`
groupId. Maven coordinates change from `io.quarkiverse.morphium:quarkus-morphium:1.2.0`
to `de.caluga:quarkus-morphium:${morphium.version}` (currently `6.3.0-SNAPSHOT`); no
package renames, no API changes. The extension now builds and releases in lockstep
with the Morphium core it depends on, instead of tracking a separately-versioned
Morphium release. The Morphium core has zero compile- or runtime dependency on this
module — building the reactor with `-DskipExtensions` produces an unchanged core.
Distribution moves from the interim GitHub Packages registry to Morphium's regular
Maven Central release pipeline; the standalone repository's own CI workflow, issue
templates, and PR template are retired in favor of the main repository's.

## [1.2.0]

### Added
- **Default `MorphiumId` JSON serialization** – the extension now ships a Jackson
  `ObjectMapperCustomizer` and a JSON-B `JsonbConfigCustomizer` that (de)serialize
  `de.caluga.morphium.driver.MorphiumId` as its canonical 24-character hex string in
  both directions. Outgoing entities with `@Id MorphiumId id` emit `"id":"<hex>"`, and
  REST endpoints accepting a `MorphiumId` path/query/body parameter parse the hex string
  back into a real `MorphiumId`. No user-written serializer is required. Registration is
  automatic and gated on the JSON layer actually present on the classpath (`quarkus-jackson`
  and/or `quarkus-jsonb`, both optional dependencies); apps that emit no JSON are unaffected.

### Changed
- **BREAKING (positive):** `MorphiumId` is now serialized as a hex string by default
  instead of the internal bean shape `{"pid":..,"counter":..,"machineId":..,"bytes":"..","time":..}`.
  Consumers that explicitly parsed the old struct must update their clients; none should —
  the old form was unusable as an id key.

  **Motivating incident:** in the datona-component-library showcase
  (`/components/tables/column-types`), the bean-walked struct made frontend grids
  (AG-Grid, MUI DataGrid) call `String(row.id)` and receive the literal `"[object Object]"`.
  Every row collapsed to the same key, the grid lost row identity and re-rendered every
  cell on each change-detection tick — flicker, lost focus, runaway memory growth, and
  an eventual renderer-process crash (Chromium exit code 5). The consumer-side fix was a
  one-line serializer; shipping it in the extension prevents every consumer from hitting
  the same bug. The hex string is the canonical, only public wire form of an id.

### Added (CosmosDB)
- **CosmosDB graceful degradation** – `@MorphiumTransactional` interceptor auto-detects
  Azure CosmosDB via Morphium's `isCosmosDB()` driver API and skips transaction wrapping;
  individual operations remain atomic, only multi-document rollback is unavailable
- Detection cached at startup via `@PostConstruct`; defensive fallback catches
  `UnsupportedOperationException` from `startTransaction()` if detection was missed
- Uses JBoss Logging (Quarkus idiomatic) instead of SLF4J
- Added "CosmosDB Compatibility" section to `transactions.adoc`

### Changed
- **BREAKING:** Config prefix changed from `morphium.*` to `quarkus.morphium.*` to follow
  Quarkus extension conventions. Rename all `morphium.` properties in your
  `application.properties` to `quarkus.morphium.` (e.g. `morphium.database` becomes
  `quarkus.morphium.database`). Dev Services keys (`quarkus.morphium.devservices.*`) are
  unchanged.
- LICENSE copyright updated from `Bardioc1977` to `The Quarkiverse Authors`

### Added
- **SSL/TLS configuration** – `quarkus.morphium.ssl.*` properties for encrypted connections,
  X.509 client-certificate authentication, keystore/truststore paths, and hostname verification
- **Health checks** – MicroProfile liveness (`/q/health/live`), readiness (`/q/health/ready`),
  and startup (`/q/health/started`) probes registered automatically via SmallRye Health;
  readiness includes connection pool metadata (connectionsInUse, threadsWaiting, per-host counts);
  disable with `quarkus.morphium.health.enabled=false`
- **Blocking call detector** – `MorphiumBlockingCallDetector` warns (throttled to 30s intervals)
  when Morphium write operations are called from Vert.x event-loop threads; suggests
  `@RunOnVirtualThread` or `@Blocking` as fix
- **Dev Services replica-set mode** – `quarkus.morphium.devservices.replica-set=true` starts
  MongoDB as a single-node replica set via `MongoDBContainer`, enabling multi-document
  transactions in dev/test mode
- **Dev UI card** – displays MongoDB connection info (hosts, database, mode, container ID,
  status) in the Quarkus Dev UI at `/q/dev-ui/`
- **Hot-reload entity cache clearing** – `ObjectMapperImpl.clearEntityCache()` called on
  Morphium creation to avoid stale class references after Quarkus live reload
- **Antora documentation** – comprehensive multi-page documentation site (9 pages) with
  GitHub Pages deployment via GitHub Actions workflow
- GitHub Packages Maven registry for artifact distribution (interim until Maven Central)
- SNAPSHOT auto-deploy on push to main
- Apache 2.0 copyright headers in all Java source files
- POM metadata: `<scm>`, `<licenses>`, `<developers>`, `<url>`, `<issueManagement>`
- Governance files: `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`
- GitHub templates: issue templates, PR template, `CODEOWNERS`, `dependabot.yml`
- `.editorconfig` for consistent code style
- `keywords` metadata in `quarkus-extension.yaml`
- `@MorphiumTransactional` CDI interceptor for declarative transaction management –
  automatically calls `startTransaction()` / `commitTransaction()` / `abortTransaction()`
- Transaction lifecycle CDI events (`MorphiumTransactionEvent`) with `@MorphiumTxPhase` qualifier:
  `BEFORE_COMMIT`, `AFTER_COMMIT`, `AFTER_ROLLBACK` (includes the causing exception)
- Initial implementation of the Quarkus Morphium extension
- `@ApplicationScoped` CDI producer for `Morphium` via `MorphiumProducer`
- Type-safe runtime configuration via `@ConfigMapping(prefix = "quarkus.morphium")`:
  - `quarkus.morphium.hosts` – MongoDB host list (default: `localhost:27017`)
  - `quarkus.morphium.database` – target database name
  - `quarkus.morphium.username` / `quarkus.morphium.password` – optional credentials
  - `quarkus.morphium.auth-database` – authentication database (default: `admin`)
  - `quarkus.morphium.read-preference` – read preference (default: `primary`)
  - `quarkus.morphium.create-indexes` – automatic index creation on startup (default: `true`)
  - `quarkus.morphium.max-connections` – connection pool size (default: `250`)
  - `quarkus.morphium.atlas-url` – optional MongoDB Atlas connection string (overrides `hosts`)
  - `quarkus.morphium.driver-name` – Morphium driver (default: `PooledDriver`; use `InMemDriver` for tests)
  - `quarkus.morphium.cache.global-valid-time` – query cache TTL in ms (default: `60000`)
  - `quarkus.morphium.cache.read-cache-enabled` – enable/disable query cache (default: `true`)
- Build-time ClassGraph scan: automatic GraalVM reflection registration for all
  `@Entity` and `@Embedded` annotated classes (no manual `reflect-config.json` required)
- Graceful shutdown via `@Observes ShutdownEvent` – `Morphium.close()` called automatically
- Java 25 compatible: no `sun.*` imports, no `Unsafe` access, no `--add-opens` for internal APIs
- `InMemDriver` support for `@QuarkusTest` without a running MongoDB instance
- Quarkus 3.32.1 support
- Dev Services: automatic MongoDB container start in dev and test mode via Testcontainers
  (`quarkus.morphium.devservices.*` config group; disabled when `quarkus.morphium.hosts` is set explicitly)
