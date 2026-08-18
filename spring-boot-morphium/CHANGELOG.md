# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased] - 1.0.0-SNAPSHOT

### Added
- Spring Boot 3.4.13 auto-configuration for Morphium (`morphium.*` properties)
- Jakarta Data 1.0 repository support via JDK dynamic proxies
  - `CrudRepository<T,K>` and `MorphiumRepository<T,K>`
  - Query derivation: `findBy*`, `countBy*`, `existsBy*`, `deleteBy*`
  - JDQL via `@Query` annotation
  - `@Find` / `@Delete` with `@By` parameter binding
  - Pagination (`Page<T>`, `CursoredPage<T>`, `PageRequest`)
  - Sorting (`Sort<T>`, `Order<T>`, `@OrderBy`)
  - Stream and async (`Stream<T>`, `CompletionStage<T>`) return types
- `@EnableMorphiumRepositories` annotation for repository scanning
- `@MorphiumTransactional` AOP aspect for declarative transactions
- Actuator health indicator (`/actuator/health` with Morphium connection details)
- `@MorphiumTest` composite test annotation (InMemDriver, no MongoDB required)
- Connection retry logic with linear backoff for transient failures
- SSL/TLS support via `morphium.ssl.*` properties

### Changed
- Renamed Maven artifacts to follow the Spring Boot starter naming convention
  (`<project>-spring-boot-*`, the `spring-boot-` prefix being reserved for Spring's
  own starters): `spring-boot-morphium-parent` → `morphium-spring-boot-parent`,
  `spring-boot-morphium-autoconfigure` → `morphium-spring-boot-autoconfigure`,
  `spring-boot-morphium-starter` → `morphium-spring-boot-starter`,
  `spring-boot-morphium-test` → `morphium-spring-boot-test`. `groupId` (`de.caluga`)
  and Java package names (`de.caluga.morphium.spring.*`) are unchanged.
- Renamed the `@ConfigurationProperties` prefix from `spring.morphium.*` to
  `morphium.*` -- the `spring.*` namespace is reserved for Spring Boot's own
  configuration keys.

