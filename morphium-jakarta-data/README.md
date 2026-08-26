# Morphium Jakarta Data

An optional module of [Morphium](https://github.com/sboesebeck/morphium), the MongoDB ODM and messaging framework for Java 21+. This module provides a framework-agnostic [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) runtime — repository implementation, query derivation, JDQL parsing, pagination, and sorting — on top of Morphium.

## What this module is and is not

`morphium-jakarta-data` is the shared implementation layer that turns Jakarta Data repository interfaces into Morphium queries. It has **zero framework dependencies**: only Morphium core and the Jakarta Data API.

Application code typically does **not** depend on this module directly. Instead, it goes through a framework integration:

| Framework | Module | Repository generation |
|-----------|--------|------------------------|
| Quarkus | `quarkus-morphium` | Gizmo bytecode generation (build-time) |
| Spring Boot | `spring-boot-morphium` | JDK dynamic proxies (runtime) |

This module exists as a separate artifact so the ~2400 lines of query derivation, JDQL parsing, pagination, and result-type handling are implemented once and shared, instead of being duplicated between the Quarkus and Spring Boot adapters.

The direct target audience for this module is anyone building their **own** framework integration — Micronaut, Helidon, plain Jakarta EE, or a hand-rolled repository wiring in plain Java. If that is not your situation, use `quarkus-morphium` or `spring-boot-morphium` instead and treat this module as an implementation detail.

## Optionality

Morphium core (`de.caluga:morphium`) does **not** depend on this module. Projects that only pull in `de.caluga:morphium` get the ODM, driver, caching, and messaging — but no `jakarta.data-api` dependency and no repository support. Jakarta Data support is opt-in by adding `morphium-jakarta-data` (directly, or transitively via one of the framework integrations).

## Features

- `CrudRepository<T, K>` and `MorphiumRepository<T, K>` base interfaces
- Query derivation from method names: `findBy*`, `countBy*`, `existsBy*`, `deleteBy*`
  - Supported operators: equals, greaterThan, lessThan, like, in, between, not, and, or
- JDQL (Jakarta Data Query Language) support via `@Query` annotation
- `@Find` / `@Delete` with `@By` parameter binding
- Pagination: `Page<T>`, `CursoredPage<T>`, `PageRequest`
- Sorting: `Sort<T>`, `Order<T>`, `@OrderBy`
- Stream and async return types: `Stream<T>`, `CompletionStage<T>`
- `RepositoryMetadata` for entity type, ID type, and collection name resolution

## Maven Dependency

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium-jakarta-data</artifactId>
  <version>${project.version}</version> <!-- currently 6.3.8-SNAPSHOT -->
</dependency>
```

The version tracks Morphium's version lockstep — `morphium-jakarta-data` is released alongside `morphium` core with the same version number, not independently.

## Architecture

```
morphium-jakarta-data
  de.caluga.morphium.data
    AbstractMorphiumRepository   Core CRUD implementation (protected setMorphium)
    MorphiumRepository           Extended repository interface (distinct, query access)
    RepositoryMetadata           Entity type, ID type, collection name metadata
    QueryDescriptor              Parsed query representation (field, operator, value)
    MethodNameParser             Parses findByXxx method names into QueryDescriptors
    JdqlParser / JdqlQuery       JDQL (Jakarta Data Query Language) parsing
    QueryMethodBridge            Executes derived queries (findBy*, countBy*, deleteBy*)
    JdqlMethodBridge              Executes @Query JDQL methods
    FindMethodBridge              Executes @Find / @Delete annotated methods
    QueryExecutor                 Low-level Morphium query execution
    QueryResultHelper             Result type adaptation (List, Stream, Page, Optional)
    CursorHelper                  Cursor-based pagination support
    SortMapper                     Maps Jakarta Data Sort/Order to Morphium sort
    MorphiumPage                   Page/CursoredPage implementation
```

### Processing chain

A repository method call is resolved through a fixed pipeline, regardless of which bridge parses it:

```
Repository method call
  -> MethodNameParser (findBy*/countBy*/...)  or  JdqlParser (@Query / JDQL)
  -> QueryDescriptor            (parsed field/operator/value/sort representation)
  -> QueryExecutor              (builds and runs the Morphium Query<T>)
  -> QueryResultHelper          (adapts the raw result to the declared return type)
  -> return type                (T, Optional<T>, List<T>, Stream<T>, Page<T>, CursoredPage<T>, CompletionStage<T>, ...)
```

`@Find` / `@Delete` methods go through `FindMethodBridge` instead of `MethodNameParser`, but join the same `QueryDescriptor` → `QueryExecutor` → `QueryResultHelper` chain from that point on.

The key design point is `AbstractMorphiumRepository.setMorphium(Morphium)` being `protected` — framework subclasses override it to bridge their injection mechanism:
- Quarkus: `@Inject` + `@PostConstruct`
- Spring Boot: public setter called by `FactoryBean`

## Building your own framework integration

To wire a new framework to this module, extend `AbstractMorphiumRepository<T, K>` for each repository interface and call `setMorphium(Morphium)` once a `Morphium` instance is available from your framework's dependency injection (or from plain code). The repository interface methods delegate to the `doXxx()` methods already implemented on `AbstractMorphiumRepository`; for query-derivation and JDQL methods not covered by the base class, dispatch through `QueryMethodBridge` / `JdqlMethodBridge` / `FindMethodBridge` as needed.

Minimal example without any framework, wiring a repository by hand:

```java
import de.caluga.morphium.Morphium;
import de.caluga.morphium.data.AbstractMorphiumRepository;
import de.caluga.morphium.data.RepositoryMetadata;

public class PersonRepositoryImpl extends AbstractMorphiumRepository<Person, String>
        implements PersonRepository {

    public PersonRepositoryImpl(Morphium morphium) {
        super(new RepositoryMetadata(Person.class, String.class, "id"));
        setMorphium(morphium);
    }

    @Override
    public Optional<Person> findById(String id) {
        return doFindById(id);
    }

    @Override
    public List<Person> findAll() {
        return doFindAll().toList();
    }
}
```

`setMorphium(Morphium)` is `protected`, so it can only be called from within the class hierarchy — subclasses either widen its visibility (as Spring Boot's public setter does) or call it internally from a constructor/lifecycle callback (as the example above and the Quarkus `@PostConstruct` integration do).

## Building

This module is part of the Morphium multi-module Maven build. Build it from the root of the `morphium` repository:

```bash
mvn -pl morphium-jakarta-data -am verify
```

`-am` (also-make) ensures `morphium-core` is built first if it is not already up to date in the reactor.

## Requirements

| Requirement | Version |
|-------------|---------|
| Java | 21+ |
| Morphium | same version (lockstep) |
| Jakarta Data API | 1.0 |

## License

This module is licensed under the same terms as the Morphium project (Apache License 2.0). There is no separate license file for this module — the license is defined at the repository root of the Morphium project.
