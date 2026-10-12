# Deprecation Policy

Morphium versions are `MAJOR.MINOR.PATCH`. This page describes when API gets deprecated,
what a deprecation has to look like, and when it disappears again.

It applies to everything users compile against: `morphium-core` and the extension modules
(`poppydb`, `morphium-jakarta-data`, `quarkus-morphium`, `spring-boot-morphium`).

## Rule 1: Deprecations can happen any time, but they have to name the successor

A deprecation can be introduced in any release, including a patch release. Deprecating
something breaks nobody, it only adds a compiler warning, and the earlier the warning
shows up the more time people have to react.

The price for that freedom: a deprecation has to answer the question "and what do I use
instead?" right where the user sees it, which is the Javadoc.

If there is a replacement, name it and show it:

```java
/**
 * @deprecated since 6.3, use {@code connectionSettings().setDatabase(db)} instead:
 *             <pre>
 *             cfg.setDatabase("mydb");                          // old
 *             cfg.connectionSettings().setDatabase("mydb");     // new
 *             </pre>
 */
@Deprecated(since = "6.3", forRemoval = true)
public void setDatabase(String db) { ... }
```

If there is no replacement, say why not. "The feature is gone" is a reason, "it never
worked and could not be fixed" is a reason, "we forgot to write one" is not:

```java
/**
 * @deprecated since 6.4. The buffered writer decides the flush interval on its own
 *             now, the setting has been ignored since 6.2. No replacement, drop the
 *             call.
 */
@Deprecated(since = "6.4", forRemoval = true)
public void setFlushGranularity(int g) { ... }
```

(Both snippets are illustrations, not verbatim quotes from the codebase.)

Formal minimum for every deprecation:

- `@Deprecated(since = "<version>", ...)`, `since` is not optional
- a Javadoc `@deprecated` tag with the replacement or the reason there is none
- an entry in `CHANGELOG.md` under `### Deprecated`
- our own code no longer uses the deprecated member, otherwise the build warns about
  itself and nobody looks at warnings anymore

A bare `@Deprecated` with no Javadoc is not a deprecation, it is a bug. Fix it or remove
the annotation.

## Rule 2: A major release removes the deprecated API

Default expectation: **after a major release, there is no deprecated API left.**

`forRemoval = true` is therefore the normal case when deprecating something. Use
`forRemoval = false` only for the exceptions described in rule 3.

The reasoning is simple. A major release contains API changes anyway, that is what makes
it a major release. Everybody who upgrades across a major expects to sit down with a list
of TODOs and work through it. That is exactly the moment where removing the old stuff is
cheapest, because the migration is already happening. If we let that moment pass, the
deprecated members survive into the next major, and the one after that, and then nobody
dares to touch them at all.

So: crossing a major is the forced cleanup, for us and for the users.

## Rule 3: Keeping a deprecation past a major needs a written reason

There are members that stay deprecated forever, and that is fine as long as the reason is
documented. The typical case is a deprecation we do not own. `MongoType.UNDEFINED`,
`DB_PTR`, `SYMBOL` and `JAVASCRIPT_SCOPE` mirror BSON types that the BSON spec itself
deprecated. We cannot remove them, we still have to decode documents that contain them.
They carry `forRemoval = false` and a Javadoc line saying so.

Rules for keeping one:

- the Javadoc says why it survives, not just that it is deprecated
- it is marked `forRemoval = false`
- the migration guide of that major mentions it under "stays deprecated"

"Somebody out there might still use it" is not a reason. That is what the deprecation
cycle was for.

## Checklist: deprecating something

- [ ] `@Deprecated(since = "<version>", forRemoval = true)` (or `false` plus a reason, see rule 3)
- [ ] Javadoc `@deprecated` names the replacement, with a before/after snippet if the call shape changes
- [ ] the replacement exists, is public and is covered by tests
- [ ] `CHANGELOG.md` entry under `### Deprecated`
- [ ] all internal call sites moved over to the replacement

## Checklist: preparing a major release

- [ ] list everything: `grep -rn "@Deprecated" --include='*.java' */src/main/java`
- [ ] remove everything marked `forRemoval = true`
- [ ] for each remaining one: remove it, or document the reason it stays (rule 3)
- [ ] the migration guide (`docs/howtos/migration-vX-to-vY.md`) lists every removal with
      its replacement, so the compile errors people hit are searchable
- [ ] afterwards: no `forRemoval = true` left in `src/main/java`

## What this means when you upgrade

Minor and patch upgrades never remove API. Deprecation warnings that appear there are the
cheap warning shot: you can fix them at your own pace, with both old and new code
compiling side by side.

Major upgrades turn those warnings into compile errors. If you kept the warning list at
zero during the minor releases, a major upgrade is mostly a version bump. If you ignored
them for two years, the major is where the bill arrives. The migration guide for that
version is the map.

## See Also

- [Upgrade v6.2 → v6.3](./howtos/migration-v6_2-to-v6_3.md)
- [Upgrade v6.1 → v6.2](./howtos/migration-v6_1-to-v6_2.md)
- [Migration v5 → v6](./howtos/migration-v5-to-v6.md)
- [API Reference](./api-reference.md)
