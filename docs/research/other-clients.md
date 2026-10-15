# How other MongoDB clients handle update values, Jakarta Data and atomic updates

Research note, 2026-10-15. Input: an initial, unsourced list of claims about other clients ("from memory"), which this note verifies. No production code was changed.

Scope: for each client, what it does for

- **(a)** mapper/codec use on update *values* and field-name translation in update operators,
- **(b)** Jakarta Data integration,
- **(c)** atomic update / upsert / find-and-modify.

Clients: Spring Data MongoDB, official MongoDB Java driver, Hibernate Data Repositories, Eclipse JNoSQL,
Quarkus Panache MongoDB, Micronaut Data MongoDB.

## Evidence legend

| Tag | Meaning |
|---|---|
| **[S]** | Read as raw source / Javadoc-bearing source file (GitHub raw, Maven Central) during this research. Class and method names are exact. |
| **[D]** | Taken from a documentation page through a summarising fetch tool. Facts are likely right but the wording was not read verbatim; re-check before quoting. |
| **[I]** | Inference from [S]/[D] material, not stated by the source. |
| **[U]** | Not verified. Listed again in the final section. |

Versions are given where the source showed one. "main" means the default branch as fetched on 2026-10-15
(Spring Data MongoDB `main` = 5.2.0-SNAPSHOT, Quarkus `main` = 999-SNAPSHOT, Micronaut Data `5.3.x`).
Downloaded source files were stored in a throw-away directory (`/tmp/oc-research-50179`) and were only read, never built
or executed.

Options **A to E** are the candidate serialisation approaches:

| Id | Option |
|---|---|
| A | Mapper-aware update builder (`query.update().set()...`), executed by an extended `findOneAndUpdate` |
| B | Minimal fix: marshal every operator map inside `Query.findOneAndUpdate`; make `MorphiumWriterImpl.marshallIfNecessary` handle any `Iterable`, array and nested Map |
| C | POJO fallback in `BsonEncoder` through a pluggable converter hook |
| D | `@Embedded` inherits `translateCamelCase` from the owning `@Entity` |
| E | Startup/lint check for an `@Embedded` with default `translateCamelCase` under `@Entity(translateCamelCase=false)` |

## 0. Morphium baseline (this checkout, for comparison)

Verified by reading the code in this checkout:

- `Query.findOneAndUpdate(Map, boolean upsert, boolean returnNew)` (`morphium-core/.../query/Query.java:493`) builds a
  `FindAndModifyMongoCommand` with `Doc.of(effectiveUpdate)`. No marshalling, no field-name translation. Only the
  generated `_id` for upserts is added (`addGeneratedIdOnInsertIfNeeded`).
- `MorphiumWriterImpl.set()` (~line 2352) translates keys with `getARHelper().getMongoFieldName(cls, key)` and calls
  `marshallIfNecessary()` (line 2733). That helper handles `Enum`, a single `@Entity`/`@Embedded`, `List` and `Map`.
  `Set`, arrays and nested maps are not covered.
- `ObjectMapperImpl.marshallIfNecessary(Object)` (line 301) handles `Expr`, entity, array, `Iterable`, `Map`. It is on
  the impl class, not on the `MorphiumObjectMapper` interface.
- `BsonEncoder` ends in `throw new RuntimeException("Unhandled Data type: ...")` (line 379).
- `MorphiumRepository` (jakarta-data module) already exposes `Morphium morphium()` (line 72) and `Query<T> query()`
  (line 81). A Morphium-specific atomic method on a repository can therefore already be written by hand through
  `query().findOneAndUpdate(...)`.
- No `jakarta.data-tck` dependency in any `pom.xml` of this checkout (grep over `pom.xml` files) **[S]**.

## 1. Spring Data MongoDB

Versions: reference docs 5.1.1 **[D]**; source `main` is 5.2.0-SNAPSHOT (`pom.xml`) **[S]**.

### (a) Mapper on update values and field names

- `UpdateDefinition` (interface in `core.query`; implemented by `Update` and `AggregationUpdate`) is the carrier;
  `MongoTemplate` never sends it unmapped **[S]**.
- `org.springframework.data.mongodb.core.convert.UpdateMapper extends QueryMapper` **[S]**. It is constructed with a
  `MongoConverter`.
  - Keys: `createPropertyField()` returns a `MetadataBackedUpdateField` that resolves the key against the
    `MongoPersistentEntity` (so `@Field` names and nested paths apply) and keeps positional `$` parts
    (`PositionParameterRetainingPropertyKeyConverter`). If the path cannot be resolved the key is kept unchanged
    (`getMappedKey()` returns `key` when `getPath() == null`) **[S]**.
  - Values: `delegateConvertToMongoType(source, entity)` calls `converter.convertToMongoType(source, typeHint)`; the
    Javadoc says it retains the original type information of the source type (this is where `_class` hints come from)
    **[S]**. `getMappedUpdateModifier` / `getMappedModifier` map the `$each`, `$position`, `$sort` modifiers of
    `push`/`addToSet` and convert association values **[S]**.
  - `getMappedObject(Bson, entity)`: if a document mixes operators (`$...`) and plain field names, the plain fields
    are moved into `$set`. `UpdateMapper.isUpdateObject(Document)` = "any top-level key starts with `$`" **[S]**.
- `UpdateContext` (inner class of `QueryOperations`): `getMappedUpdate(entity)` runs the mapper;
  `increaseVersionForUpdateIfNecessary(entity)` adds `$inc` on the `@Version` field if the update does not already
  modify it **[S]**. `MongoTemplate.doFindAndModify` and `doUpdate` both call these two methods **[S]**.
- Field naming is a property of the mapping context, not of the owning type: `MongoMappingContext` holds one
  `FieldNamingStrategy` (default `PropertyNameFieldNamingStrategy.INSTANCE`), overridden per property by `@Field`
  **[S]**. Nothing depends on the owner of an embedded object **[I]**.

### (b) Jakarta Data

- Zero code-search hits for `"jakarta.data"` in `spring-projects/spring-data-commons`, `spring-data-mongodb` and
  `spring-data-jpa` (GitHub code search, 2026-10-15) and no path containing `jakarta/data` in the trees of
  `spring-data-commons` and `spring-data-mongodb` **[S]**. Conclusion: no Jakarta Data implementation in Spring Data
  **[I]**; a project announcement or a separate repository could still exist **[U]**.
- Morphium therefore has no Spring Data precedent for Jakarta Data; its own `spring-boot-morphium` adapter is the
  precedent.

### (c) Atomic update, upsert, find-and-modify

- `MongoTemplate.findAndModify(Query, UpdateDefinition, [FindAndModifyOptions,] Class[, collection])` ->
  `doFindAndModify(...)` maps query and update, then `FindAndModifyCallback` builds the driver's
  `FindOneAndUpdateOptions` (sort, upsert, projection, `ReturnDocument.AFTER` if `returnNew`, collation,
  `arrayFilters`) and calls `collection.findOneAndUpdate(query, document|pipeline, opts)` **[S]**.
- `Update` API: `set`, `setOnInsert`, `inc`, `multiply`, `min`, `max`, `rename`, `currentDate`, `push`/`addToSet`
  with `.each(...)`, `.atPosition(...)`, `pop`, `pull`, `pullAll`, `unset` **[D]**.
- Fluent: `template.update(Person.class).matching(query).apply(update).withOptions(
  FindAndModifyOptions.options().upsert(true).returnNew(true)).findAndModifyValue()` **[D]**.
- `AggregationUpdate` (`Aggregation.newUpdate()`, stages `set(..).toValue(..)`, `unset`, `replaceWith`) maps to a
  pipeline update; `MongoTemplate` branches on `updateContext.isAggregationUpdate()` **[S]**/**[D]**.
- Repositories **[D]** (`modifying-methods` page): `@Update("{ '$inc': { 'visits': 1 } }")` and
  `@Update(pipeline = {...})` on a derived or `@Query` method; `?1` / `?#{...}` placeholders; return types `void` or a
  numeric (number of modified documents); applied to **all** matching documents, not limitable by `Page`/limit;
  "repository updates do not emit persistence or mapping lifecycle events". The `@Update` annotation has attributes
  `value`/`update` (aliases) and `pipeline` **[S]**. Parameters like an `Address` object are bound through the converter
  (`@Update("{ '$push': { 'shippingAddresses': ?1 } }")` with `Address` parameter) **[D]**.
- Upsert through `template.update(..).apply(..).upsert()`; `upsert` has no sort, `findAndModify` does **[D]**.
- E11000 is translated to `org.springframework.dao.DuplicateKeyException` in `MongoExceptionTranslator` **[S]**.

## 2. Official MongoDB Java driver

Versions: `mongodb-driver-sync` latest on Maven Central 5.13.0 (metadata, 2026-10-15) **[S]**; source read from `main`.

### (a) Codec on update values, no field-name translation

- `com.mongodb.client.model.Updates` builds `Bson` objects. `SimpleUpdate.toBsonDocument(Class, CodecRegistry)` and
  `WithEachUpdate.toBsonDocument(...)` write the operator and the **field-name string unchanged**, then call
  `BuildersHelper.encodeValue(writer, value, codecRegistry)` **[S]**.
- `BuildersHelper.encodeValue`: `null` -> `writeNull`; a `Bson` value -> `toBsonDocument(...)`; anything else ->
  `codecRegistry.get(value.getClass()).encode(...)`. A POJO therefore works as soon as the registry contains a
  `PojoCodecProvider` (or a record codec), and fails with `CodecConfigurationException` when it does not **[S]**.
- Field names in `Updates`/`Filters` are plain strings: the driver does **not** map Java property names to
  `@BsonProperty` names in these builders **[S]**/**[I]** (no typed-field API found in `Updates.java`). The
  `@BsonProperty` rename applies only when the codec encodes the POJO value itself **[D]**.
- Where the registry is applied: `Operations.findOneAndUpdate` calls `toBsonDocument(update)` / `toBsonDocument(filter)`
  with the collection's registry **[S]**.
- POJO codec facts **[D]** (docs "POJO Customization", v5.x): `PojoCodecProvider.builder()`, annotations `@BsonId`,
  `@BsonProperty` (field-name mapping), `@BsonDiscriminator`, `@BsonIgnore`, `@BsonCreator`, `@BsonRepresentation`,
  `@BsonExtraElements`; conventions `ANNOTATION_CONVENTION`, `CLASS_AND_PROPERTY_CONVENTION`, `DEFAULT_CONVENTIONS`.
- Records: `bson-record-codec` module with `RecordCodecProvider`/`RecordCodec` exists on `main` and on Maven Central
  (first listed release 4.6.0) **[S]**.

### (b) Jakarta Data

Not part of the driver. Out of scope **[I]**.

### (c) Atomic update / upsert / find-and-modify

- `MongoCollection.findOneAndUpdate(filter, Bson update | List<Bson> pipeline[, FindOneAndUpdateOptions])` **[S]**.
- `FindOneAndUpdateOptions` (all setters seen in source): `projection`, `sort`, `upsert`, `returnDocument`
  (default `ReturnDocument.BEFORE`), `maxTime`, `bypassDocumentValidation`, `collation`, `arrayFilters`, `hint`,
  `hintString`, `comment`, `let` **[S]**.
- `Updates` operator set: `combine`, `set`, `unset`, `setOnInsert`, `rename`, `inc`, `mul`, `min`, `max`,
  `currentDate`, `currentTimestamp`, `addToSet`, `addEachToSet`, `push`, `pushEach` (+ `PushOptions`), `pull`,
  `pullByFilter`, `pullAll`, `popFirst`, `popLast`, bitwise ops **[S]**.
- Upsert + filter mismatch on an existing document raises a `MongoCommandException` / `DuplicateKey` error from the
  server (E11000). The driver adds no typed "lost" result **[I]** (server behaviour, not read in driver source).

## 3. Hibernate Data Repositories

Versions: docs 7.4 and 8.0 read; Maven Central: `hibernate-core` latest 8.0.0.Beta3 (updated 2026-10-11), 7.4.13.Final
exists **[S]**.

- (a) Relational only. The overview page states it is "an implementation of Jakarta Data targeting relational
  databases and backed by Hibernate ORM"; persistence is delegated to a Hibernate `StatelessSession`; the Hibernate
  Processor generates the implementation at compile time and checks queries (HQL/JDQL) at compile time **[D]**.
  Field-name translation is the ORM's concern (attribute -> column), not comparable to a document store **[I]**.
- (b) Targets **Jakarta Data 1.0** in both the 7.4 and 8.0 docs. The 8.0 text says 1.0 has no API for programmatic
  restrictions; Hibernate offers its own `org.hibernate.query.restriction.Restriction` and says "restrictions will be
  standardized by Jakarta Data 1.1". Reactive repositories are described as an extension "being written into Data 1.1"
  **[D]**. The docs do not mention `@Is` or constraints **[D]**.
- (c) `@Insert`/`@Update`/`@Delete`/`@Save` map one-to-one to `StatelessSession.insert()/update()/delete()/upsert()`,
  no cascading **[D]**. A `@Query` method may perform `update`, `delete` or `insert` statements and must return
  `void`, `int` or `long` (affected rows) **[D]**. So the "conditional update, return count" pattern exists in HQL;
  there is no find-and-modify and no returned document **[D]**/**[I]**.
- MongoDB: none. Hibernate OGM (older, MongoDB backed) was not examined **[U]**.
- Relevance for Morphium: the **processor + compile-time checked JDQL/HQL update returning a count** pattern, and the
  documented stance "`@Save` = upsert of the whole entity".

## 4. Eclipse JNoSQL

Versions: Maven Central `org.eclipse.jnosql:jnosql` and `jnosql-mongodb` latest 1.2.0-M2 (2026-10-08). Tag
`1.2.0-M2` pins `jakarta.data-api` **1.1.0-M4** and `jakarta.nosql` 1.1.0-M3; tag `1.1.20` pins `jakarta.data-api`
**1.0.2** and `jakarta.nosql` 1.0.0 (root `pom.xml` properties) **[S]**. `jnosql-mongodb` uses `mongodb-driver-sync` through
the property `monbodb.driver` (sic); the exact version was not read **[U]**.

### (a) Mapper and field names

- The MongoDB module converts entity -> `CommunicationEntity` (name/value `Element`s) in the mapping layer, then
  `MongoDBUtils.updateDocument(...)` builds `Updates.combine(Updates.set(field, convert(value)) ...)`. Value conversion
  is hand-written in `MongoDBUtils.convert` (sub-documents and lists of sub-documents -> `Map`/`Document`) **[S]**. It
  does not use a driver POJO codec **[I]**.
- Fluent `QueryMapper.MapperUpdate*` (`MapperUpdate.set(name).to(value)`) resolves names with
  `mapping.columnField(name)` and converts values through `getValue(value)` / converters **[S]**.
- Parsed JDQL `UPDATE` items resolve field names through `CommunicationObserverParser.fireSelectField(entity, name)`
  **[S]**.

### (b) Jakarta Data

- Implements Jakarta Data (repository proxy in `jnosql-mapping-semistructured`/`-core`), and follows the spec
  version listed above. Code references `jakarta.data.repository.Is` (8 hits in `eclipse-jnosql/jnosql`, e.g.
  `RepositoryParam.java`, `ParamValue.java`) and has a `RestrictionConverter` plus tests
  (`CrudRepositoryProxyRestrictionTest`) **[S]**. No hits for `jakarta.data.repository.Assign` **[S]**; the
  `jakarta.data-api-1.1.0-M4.jar` listing has no `Assign` class either (see section 8).
- **JDQL `UPDATE` is parsed**: grammar `JDQL.g4` has `update_statement : UPDATE entity_name set_clause where_clause?`
  with `update_item : state_field_path_expression EQ (scalar_expression | NULL)`; `UpdateParser` then **rejects
  arithmetic and parentheses** with `UnsupportedOperationException("Eclipse JNoSQL does not support arithmetic
  operations in the UPDATE clause: ...")` **[S]**. So `SET x = x + 1` (needs `$inc`) is not supported.

### (c) Atomic updates

- `MongoDBDocumentManager.update(CommunicationEntity)` = `updateOne(idFilter, $set of all elements)`, requires an
  `_id` element. `update(UpdateQuery)` = `updateMany(filter, $set ...)`. No upsert, no `findOneAndUpdate`, no
  `$inc`/`$setOnInsert`/`$addToSet` anywhere in the module's public manager API **[S]** (read: `MongoDBDocumentManager`,
  `MongoDBUtils`, `MongoDBTemplate`).
- The MongoDB-specific `MongoDBTemplate` only adds Bson-based `delete`, `select`, `aggregate`, `count` **[S]**.
  Escape hatch to the driver exists, atomic operators do not.

## 5. Quarkus Panache MongoDB

Versions: guide text mentions Quarkus 3.40.1 **[D]**; source from `main` **[S]**.

### (a) Mapper and field names

- Panache MongoDB relies on the driver `PojoCodecProvider` (also records, per the guide) **[D]**.
- `MongoOperations.bindUpdate(clazz, query, params)` has two paths **[S]**:
  - **PanacheQL** (`"firstname = ?1 and status = ?2"`): parsed with the Hibernate HQL lexer/parser,
    `MongoParserVisitor` is given `MongoPropertyUtil.getReplacementMap(clazz)`, i.e. entity field -> `@BsonProperty`
    name, so keys are translated; `$set` is added if no update operator is present (`containsUpdateOperator`).
  - **Native** (string starting with `{`): `NativeQueryBinder.bindQuery` does `Document.parse` and substitutes
    placeholders. **No field-name translation.** The guide says the same: PanacheQL uses Java field names, native
    queries use BSON names **[D]**.
- Values: `CommonQueryBinder.paramValue` converts only enums to `name()`; its Javadoc says "All other values are
  returned as-is - the driver's CodecRegistry handles encoding" **[S]**. Entity `update(entity)`/`persistOrUpdate`
  use the collection's codec (`collection.getCodecRegistry().get(entity.getClass())`, `replaceOne(..., upsert(true))`)
  **[S]**.

### (b) Jakarta Data

- `extensions/data/` in Quarkus `main` contains only `hibernate` (`deployment`, `runtime`, `processor`); there is no
  MongoDB / Panache MongoDB Jakarta Data module **[S]** (tree listing). `quarkus-morphium` is thus the only Mongo-backed
  Jakarta Data extension seen here **[I]**.

### (c) Atomic updates

- `Person.update("name", "Mortal").where("status", Status.Alive)` -> `PanacheUpdate`; `PanacheUpdateImpl.executeUpdate`
  = `collection.updateMany(query, update).getModifiedCount()` (with optional `ClientSession`). Returns a count **[S]**.
- `persistOrUpdate(entity)` = `replaceOne(idFilter, entity, ReplaceOptions().upsert(true))`; the guide calls it the
  MongoDB `_upsert_` capability **[S]**/**[D]**. Whole-document replace, not a partial atomic update.
- No `findOneAndUpdate`/`findAndModify` in the Panache sources read (`MongoOperations`, `JavaMongoOperations`,
  `PanacheUpdateImpl`) **[S]**; users drop to the driver collection.

## 6. Micronaut Data MongoDB

Versions: guide 5.2.3 **[D]**; source branch `5.3.x`; `micronaut-mongo` 6.2.0; managed Jakarta Data `1.1.0-M4`
(`gradle/libs.versions.toml`) **[S]**.

### (a) Mapper and field names

- Entities use `@MappedEntity` / `@MappedProperty`; the default naming strategy is underscore-separated lower case
  (`FooBar` -> `foo_bar`), configurable per entity with `namingStrategy`, computed at compile time **[S]** (docs source
  `mongoNaming.adoc`). `PersistentProperty.getPersistedName()` uses `owner.getNamingStrategy()` **[S]**; whether an
  embedded entity inherits the parent's strategy was not checked **[U]**.
- Derived `update` methods (`void update(@Id ObjectId id, int pages)`, `void updateByTitle(String title, int pages)`)
  are generated at compile time from property names; the non-id/non-criteria parameters become the `$set` values **[D]**
  (docs source: `mongoInsertsAndUpdates.adoc`, `doc-examples/mongo-example-java/.../BookRepository.java`).
- Values are encoded through a Micronaut Data codec registry (`DataCodecRegistry`, `MappedEntityCodec`); entity writes
  use `BsonDocumentWrapper.asBsonDocument(entity, codecRegistry)` **[S]**. Whether parameter objects inside a custom
  `$set` JSON are encoded through the same registry was not traced **[U]**. Field names inside the JSON of
  `@MongoUpdateQuery` are written by the developer (example `{$set:{name: 'tom'}}`); whether they are translated is
  **[U]**.

### (b) Jakarta Data

- Documented support (`src/main/docs/guide/shared/jakartaData.adoc`): `@Repository`, `@Find`/`@By`, `@Query`,
  `@Insert`, `@Update`, `@Save`, `@Delete`, `@OrderBy`, `Limit`, `PageRequest`, offset and cursor pagination, derived
  names. You still need `@MongoRepository` to pick the backend **[S]**.
- `MongoJakartaDataExceptionConverter` exists in `data-mongodb`, and the repo has a module
  `jakarta-data-tck/mongodb` that runs the Jakarta Data TCK against MongoDB (build file read; test resources listed)
  **[S]**. Whether the TCK passes is **[U]**.
- Micronaut also has code referencing `jakarta.data.repository.Is` (4 hits) **[S]**.

### (c) Atomic updates

- `@MongoUpdateQuery(filter=..., update=...)`: count/acknowledgement result (`long`, `int`, `boolean`, `void`) **[S]**.
- `@MongoUpdateReturningQuery(update=..., filter=..., sort=..., project=..., collation=..., arrayFilters=...,
  returnDocument = ReturnDocument.BEFORE|AFTER)` (since 5.0.0, `MongoFindOneAndUpdate` marked `@Experimental`) executes
  `collection.findOneAndUpdate(clientSession, filter, update, options)` and returns one document. Documented rules:
  single-result return type only, default is BEFORE, use `sort` if the filter can match several documents, mutually
  exclusive with `@MongoUpdateQuery`. Example: `@MongoUpdateReturningQuery(filter = "{_id: {$eq: :id}}",
  update = "{$inc:{pages: 1}}", returnDocument = ReturnDocument.AFTER) Book incrementPagesAfter(ObjectId id);`
  **[S]**.
- `@MongoUpdateOptions(upsert, bypassDocumentValidation, hint, arrayFilters)` (since 3.3.0) -> upsert flag for update
  methods **[S]**.
- Optimistic locking: `@Version`, filter built by `createFilterIdAndVersion(...)`, `checkOptimisticLocking(...)` on the
  modified count **[S]**.
- This is the closest existing analogue to "a Jakarta Data repository method that is an atomic find-and-modify on
  MongoDB", and it is *not* a Jakarta Data annotation but a backend-specific one **[I]**.

## 7. Pattern -> what Morphium can adopt

Effort: S = under about 2 days, M = 2 to 5 days, L = more than a week (guess, before design work).
"Options" = which of the options A to E the pattern supports. "Area" = task area
(1 serialisation, 2 atomic ops in Jakarta Data, 3 core API gaps, 4 boilerplate, 5 learnings from clients,
6 contract tests, 7 other).

| # | Pattern (who) | What Morphium can adopt | Effort | Options | Area |
|---|---|---|---|---|---|
| 1 | **Apply the codec/mapper to every update value at the command boundary** (driver `BuildersHelper.encodeValue`; Panache `paramValue` + registry) | Marshal each value of each operator map before `FindAndModifyMongoCommand`/`UpdateMongoCommand` is filled. Interim: reuse `ObjectMapperImpl.marshallIfNecessary` (already handles `Iterable`, arrays, `Map`, `Expr`), expose it on `MorphiumObjectMapper`. Long term: a converter hook in `BsonEncoder` for hand-built commands (the driver does exactly this via the registry). | S for B, M for C | B, C | 1 |
| 2 | **Update definition mapped against entity metadata at execution time** (Spring `UpdateDefinition` + `UpdateMapper`/`UpdateContext.getMappedUpdate`) | `Query.update()` builder object (`set`, `setOnInsert`, `inc`, ...) that stores *Java* field names and raw values; at `execute()` run keys through `getMongoFieldName` (incl. nested `a.b` and positional `$`/`$[x]`) and values through the mapper. Keep unresolved keys unchanged, as Spring does. | M to L | A (also B as precursor) | 1, 3 |
| 3 | **Plain field map auto-wrapped in `$set`** (Spring `UpdateMapper.getMappedObject`; Panache adds `$set` if no operator) | Let `findOneAndUpdate(Map)` accept `{field: value}` without `$set` and wrap it; use "any top-level key starts with `$`" (Spring `isUpdateObject`) as the raw-vs-plain test. | S | A, B | 3 |
| 4 | **Idempotent marshalling instead of "raw vs mapped" flags** (Spring maps whatever it is given; unknown keys pass through) | For option B there is a concern that a pre-marshalled map is indistinguishable from raw. Spring's answer: mapping is idempotent for documents/maps, so just map again. Add a test that double marshalling of `serialize()` output is stable (watch `class_name` for non-polymorphic embeddeds). | S | B | 1 |
| 5 | **Complete, typed operator set with `each`** (Spring `Update`; driver `Updates`: `setOnInsert`, `mul`, `min`, `max`, `rename`, `currentDate`, `addEachToSet`, `pushEach` + `PushOptions`, `combine`) | Close the operator gaps in one builder rather than as separate `Query.xxx(...)` overloads: `setOnInsert`, `addToSet(...).each(...)`, `min/max/mul/rename`, correct `currentDate`. `combine`-style merging of same-operator calls (two `set`s -> one `$set`). | M | A | 3 |
| 6 | **Options object instead of boolean pairs** (driver `FindOneAndUpdateOptions` with `ReturnDocument`; Spring `FindAndModifyOptions`; Micronaut `returnDocument`) | Replace `findOneAndUpdate(Map, boolean upsert, boolean returnNew)` by `findOneAndUpdate(update, FindAndModifyOptions)` with `upsert`, `returnDocument(BEFORE/AFTER)`, `projection`, `arrayFilters`, `hint`, `collation`, `maxTime`. Old signature stays as a delegating overload. Fixes the confusing "BEFORE is default, null on upsert" documented in section 2. | S | A | 3 |
| 7 | **Version bump inside the mapped update** (Spring `increaseVersionForUpdateIfNecessary`: `$inc` on `@Version` unless the update already modifies it) | The mapper-aware builder applies `@Version` the same way. On an upsert that inserts, `$inc` on a missing field yields 1 **[I]**, which would also remove the `@Version` initialisation trap (applications set `entityVersion = 1L` by hand). Needs a test against real MongoDB. Also `@LastChange` and `@CreationTime` (`$setOnInsert`) as already done in `MorphiumWriterImpl.set`. | M | A | 3 |
| 8 | **State the lifecycle contract** (Spring docs: repository updates emit no persistence/mapping events) | Document that `findOneAndUpdate` and bulk updates skip `@PreStore`/`@PostStore`; decide which auto values (`@LastChange`, `@CreationTime`, `@Version`) the builder applies. | S | A, E (doc) | 3, 4 |
| 9 | **Typed duplicate-key failure** (Spring `MongoExceptionTranslator` -> `DuplicateKeyException`) | Add a `MorphiumDuplicateKeyException` (or flag on `MorphiumDriverException`) set from code 11000, plus a typed result for a lost conditional upsert (`acquired` / `held`) instead of callers parsing `getMongoCode()`. | S to M | A | 3 |
| 10 | **Repository method = atomic update with named parameters** (Spring `@Update("{ '$inc': ...}")`; Micronaut `@MongoUpdateQuery`, `@MongoUpdateReturningQuery`, `@MongoUpdateOptions(upsert)`) | A Morphium-specific annotation pair for `morphium-jakarta-data` and `MorphiumDataProcessor`: count-returning update and document-returning find-and-modify with `upsert`, `returnDocument`, `sort`. Parameters are bound through the mapper (pattern 1/2), not as raw JSON. Prefer a structured form (`set = {...}`, `setOnInsert = {...}`, `inc = {...}` as arrays of field/param pairs) over JSON strings, to get compile-time field-name checking like Micronaut/Hibernate's processor. Because Jakarta Data has no upsert or find-and-modify annotation (see section 8) this is a Morphium extension either way. | L | A (+ B) | 2 |
| 11 | **Derived update methods** (Micronaut `update(@Id id, int pages)`, `updateByTitle(...)`: non-criteria parameters become `$set`) | `MethodNameParser`: `updateByX(..., values)` -> `$set` through the builder, returns `long`. Fits Morphium's existing derived-query machinery. | M | A | 2 |
| 12 | **JDQL `UPDATE ... SET ... WHERE` returning a count** (Hibernate `@Query` with `void/int/long`; JNoSQL parses it, maps to `$set` + `updateMany`, rejects arithmetic) | Add `UPDATE` (and `DELETE FROM`) to `JdqlParser`. Go beyond JNoSQL: `SET x = x + :n` -> `$inc`, `x = x * :n` -> `$mul`, plain assignment -> `$set`, `= NULL` -> `$unset`; keep "conditional update, return count" semantics. Cannot express upsert or return the document. Confirm JDQL scope in `docs/research/jakarta-data-1.1.md`. | M to L | A | 2 |
| 13 | **Return-type convention** (Spring: `void`/`long`; Micronaut: `long`/`int`/`boolean` vs entity; Hibernate `void/int/long`) | Define for the Morphium annotations: numeric/`boolean`/`void` = count form, entity or `Optional<Entity>` = find-and-modify form; reject collections. | S | A | 2 |
| 14 | **Run the Jakarta Data TCK against a Mongo backend** (Micronaut `jakarta-data-tck/mongodb`) | Add the TCK to `morphium-jakarta-data` tests against PoppyDB/InMemoryDriver and real MongoDB; use it as the "contract test InMem vs real MongoDB" vehicle for the repository layer. Check the version matches the implemented API (1.0.0 today). | M | none (supports E-style safety nets) | 6 |
| 15 | **Naming strategy is global or per-class, never owner dependent** (Spring `MongoMappingContext.fieldNamingStrategy` + `@Field`; Micronaut per-entity `namingStrategy`; driver explicit `@BsonProperty`) | Evidence for the recommendation: nobody makes embedded naming depend on the owner. Prefer E (detect) and an *opt-in* tri-state D, or a global setting; not a silent behaviour change. | S (E) / L (D) | D, E | 1 |
| 16 | **Record / immutable value-object codec** (driver `bson-record-codec`; Panache docs: records supported) | Mapper support for records (and single-value converters) removes the `{"value": ...}` sub-document workaround applications use today. | L | none | 7 |
| 17 | **Escape hatch stays codec-aware** (driver: `Bson` values inside `Updates` use the same registry; Spring/Panache: raw native strings are *not* translated and say so) | Keep raw `Map` updates raw by contract, say so in the new "raw commands and embedded objects" docs page, and make the typed path (pattern 2) the recommended one. | S | E (doc), A | 4 |

### Reading the table

- Patterns 1, 3, 4 and 6 are cheap and support the recommendation **B now**.
- Patterns 2, 5, 7 and 9 together are **A** (the target API). Spring's `UpdateMapper` is the closest reference design,
  the driver's `Updates` the closest reference vocabulary.
- Patterns 10 to 13 are what no Jakarta Data implementation offers on MongoDB today; Micronaut's
  `@MongoUpdateReturningQuery` is the only precedent for find-and-modify on a repository method.
- Pattern 14 gives a verifiable acceptance test for areas 2 and 6.

## 8. Corrections to the initial claims

| Initial claim | Result |
|---|---|
| "All of them apply the mapper/codec to update *values* and translate field names for update operators" | **Partly wrong.** Values: yes for Spring (`UpdateMapper`), driver and Panache (codec registry), Micronaut for entity writes. Field names: Spring yes; **driver `Updates` no**; **Panache native updates no** (PanacheQL yes); JNoSQL yes in its own mapper layer; Micronaut custom JSON **unverified**. |
| "none leaves the user to serialise embedded objects by hand" | True for the clients with a codec registry or converter. JNoSQL's MongoDB module only handles sub-documents it knows (`MongoDBUtils.convert`) **[S]**. |
| "Hibernate Data Repositories: Jakarta Data reference implementation ... `@Query` (JDQL/HQL) incl. update statements" | Correct for relational stores; it targets Jakarta Data **1.0** in the 7.4 and 8.0 docs, and has no MongoDB path **[D]**. It is the reference for *compile-time checked JDQL/HQL*, not for document stores. |
| "Eclipse JNoSQL: compare its handling of update statements and 1.1 features" | JNoSQL parses JDQL `UPDATE` but supports only plain `SET field = value` (`$set`, `updateMany`); tag `1.2.0-M2` is built against `jakarta.data-api` 1.1.0-M4 **[S]**. |
| "Quarkus Panache MongoDB: `update(...)`, codec based POJO support. Micronaut Data MongoDB: `@Update`, `@Query` with update maps" | Panache: `update(...)` is `updateMany` with a count result; native strings are not field-translated. Micronaut: the annotations are `@MongoUpdateQuery` / `@MongoUpdateReturningQuery` / `@MongoUpdateOptions`, not `@Update` + `@Query`; Jakarta Data `@Update` is the entity-based lifecycle operation **[S]**. |
| Incidental check: Jakarta Data 1.1 and partial updates | `jakarta.data-api-1.1.0-M4.jar` (Maven Central, listed with `unzip -l`, not run) contains `jakarta/data/constraint/*` (`EqualTo`, `GreaterThan`, `In`, `Like`, `Null`, ...), `repository/Is`, `First`, `Select`, but **no `Assign` or similar update annotation**. Full 1.1 analysis belongs to `docs/research/jakarta-data-1.1.md`; the latest Central release of the API is 1.1.0-M4 (2026-08-27) and the latest final is 1.0.2. |

## 9. Not verified

- Spring Data: whether a Jakarta Data adapter exists outside the three repositories searched; exact wording of the
  reference docs (read through a summarising fetch); `@Version` initial values (0 vs 1) stated in the docs summary.
- Driver: whether a typed field-reference API exists in a newer driver than `main` at fetch time; exact behaviour and
  exception class of a conditional upsert collision (server behaviour, not traced in driver source).
- Hibernate: any MongoDB route (OGM is a separate, old project); behaviour of `@Query UPDATE` with `@Version`.
- JNoSQL: the exact `mongodb-driver-sync` version in 1.2.0-M2; whether `@Is`/restrictions are fully supported on the
  MongoDB module (only code references seen); whether `@Update` with partial values exists in any JNoSQL release.
- Panache MongoDB: guide version number (from the fetch summary); a Jakarta Data module outside `extensions/data`.
- Micronaut Data: whether custom JSON in `@MongoUpdateQuery` / `@MongoUpdateReturningQuery` translates field names or
  encodes entity parameters through the codec registry; whether embedded entities inherit the parent's naming
  strategy; whether the Mongo Jakarta Data TCK passes; behaviour of `@MongoUpdateOptions(upsert=true)` on a collision.
- All effort estimates in section 7 are guesses without design work.
- All `[D]` items: re-read the page text before quoting it in a ledger task.

## 10. Primary sources

- Spring Data MongoDB: `core/convert/UpdateMapper.java`, `core/MongoTemplate.java` (`doFindAndModify`, `doUpdate`,
  `FindAndModifyCallback`), `core/QueryOperations.java` (`UpdateContext`), `core/query/UpdateDefinition.java`,
  `repository/Update.java`, `core/mapping/MongoMappingContext.java`, `core/MongoExceptionTranslator.java`
  (`github.com/spring-projects/spring-data-mongodb`, `main`); reference docs `template-crud-operations.html`,
  `repositories/modifying-methods.html`.
- MongoDB Java driver: `driver-core/.../client/model/Updates.java`, `FindOneAndUpdateOptions.java`,
  `BuildersHelper.java`, `internal/operation/Operations.java`, `driver-sync/.../MongoCollectionImpl.java`
  (`github.com/mongodb/mongo-java-driver`, `main`); docs "POJO Customization" under
  `mongodb.com/docs/drivers/java/sync/current/data-formats/`.
- Hibernate: `hibernate.org/repositories/`, `docs.hibernate.org/orm/7.4|8.0/repositories/html_single/Hibernate_Data_Repositories.html`.
- Eclipse JNoSQL: `eclipse-jnosql/jnosql` (`JDQL.g4`, `UpdateParser.java`, `UpdateQueryParser.java`, `MapperUpdate.java`,
  root `pom.xml` at tags `main`, `1.2.0-M2`, `1.1.20`); `eclipse-jnosql/jnosql-databases`
  (`MongoDBDocumentManager.java`, `MongoDBUtils.java`, `MongoDBTemplate.java`).
- Quarkus: `extensions/panache/mongodb-panache-common/runtime/.../MongoOperations.java`,
  `binder/PanacheQlQueryBinder.java`, `binder/NativeQueryBinder.java`, `binder/CommonQueryBinder.java`,
  `runtime/PanacheUpdateImpl.java`; `extensions/data/` tree; guide `quarkus.io/guides/mongodb-panache`.
- Micronaut Data `5.3.x`: `data-mongodb/.../annotation/MongoUpdateReturningQuery.java`, `MongoUpdateOptions.java`,
  `operations/MongoFindOneAndUpdate.java`, `operations/DefaultMongoRepositoryOperations.java`,
  `src/main/docs/guide/mongo/**`, `src/main/docs/guide/shared/jakartaData.adoc`, `jakarta-data-tck/mongodb/`,
  `gradle/libs.versions.toml`.
- Maven Central metadata: `jakarta/data/jakarta.data-api`, `org/mongodb/mongodb-driver-sync`,
  `org/mongodb/bson-record-codec`, `org/hibernate/orm/hibernate-core`, `org/eclipse/jnosql/jnosql`.
