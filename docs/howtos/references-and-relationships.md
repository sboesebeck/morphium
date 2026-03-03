# References and Relationships

This guide covers Morphium's `@Reference` annotation for modeling relationships between entities, including cascade operations and circular reference handling.

## @Reference vs @Embedded

| Feature | `@Reference` | `@Embedded` |
|---|---|---|
| Storage | Only the target's ID in the parent document | Full object embedded in parent document |
| Own collection | Yes — referenced entity lives in its own collection | No — embedded within the parent |
| Independent lifecycle | Yes — can exist without the parent | No — tied to the parent document |
| Query performance | N+1 queries (one per reference) | Single read for parent + children |
| Update independence | Can be updated without touching the parent | Must update the parent document |

**Use `@Reference` when:**
- The referenced entity is shared across multiple parents
- You need independent queries on the referenced entity
- The referenced data is large or frequently updated independently

**Use `@Embedded` when:**
- The child data belongs exclusively to the parent
- You want atomic reads/writes of parent + children
- The embedded data is small and rarely queried independently

## Basic Usage

```java
@Entity
public class BlogPost {
    @Id
    private MorphiumId id;
    private String title;

    @Reference
    private Author author;

    @Reference
    private List<Comment> comments;

    @Reference
    private Map<String, Tag> tags;
}
```

In MongoDB, only the author's ID (and type info) is stored in the BlogPost document. The Author object lives in its own collection.

## automaticStore (default: true)

When `automaticStore` is enabled (the default), Morphium automatically persists referenced objects that don't yet have an ID:

```java
Author author = new Author();
author.setName("Jane");
// author has no ID yet

BlogPost post = new BlogPost();
post.setAuthor(author);

morphium.store(post);
// author is automatically stored first, then post references author's new ID
```

Set `automaticStore = false` when you want to control persistence manually:

```java
@Reference(automaticStore = false)
private Author author;

// Must store author first:
morphium.store(author);
post.setAuthor(author);
morphium.store(post);
```

## lazyLoading

With `lazyLoading = true`, referenced objects are not loaded from the database until first accessed:

```java
@Reference(lazyLoading = true)
private Author author;

// When post is loaded, author is a CGLib proxy (no DB query yet)
BlogPost post = morphium.findById(BlogPost.class, id);

// First access triggers the actual DB query:
String name = post.getAuthor().getName(); // DB query happens here
```

**Important for bidirectional references:** If entity A references B and B references A, you must use `lazyLoading = true` on at least one side to prevent infinite deserialization loops.

## cascadeDelete (default: false)

When `cascadeDelete = true`, deleting the parent entity also deletes the referenced entities:

```java
@Entity
public class Order {
    @Id
    private MorphiumId id;

    @Reference(cascadeDelete = true)
    private List<OrderItem> items;

    @Reference  // default: cascadeDelete = false
    private Customer customer;
}

// Deleting the order also deletes all items, but keeps the customer
morphium.delete(order);
```

**Key behaviors:**
- Only applies to entity-based `remove(Object)` / `delete(Object)` calls, not query-based deletes
- Cascade delete is recursive: if OrderItem also has `@Reference(cascadeDelete = true)`, those references are deleted too
- **Cycle-safe:** Circular cascade references (A→B→A) are detected and won't cause infinite loops
- Referenced objects are deleted **before** the parent to maintain referential consistency

## orphanRemoval (default: false)

When `orphanRemoval = true`, updating a parent entity will automatically delete referenced objects that are no longer referenced:

```java
@Entity
public class Team {
    @Id
    private MorphiumId id;

    @Reference(orphanRemoval = true)
    private List<Player> roster;
}

// Initial state: team references players A, B, C
team.getRoster().remove(playerB);
morphium.store(team);
// playerB is automatically deleted from the database (orphan removed)
```

**Key behaviors:**
- Only triggers on **updates** (entities with an existing ID), not inserts
- Setting a reference to `null` also triggers orphan removal for the previously referenced object
- The old version is loaded from DB before the update to determine which references were removed
- Works with single references, lists, and maps

### orphanRemoval vs cascadeDelete

| Scenario | `cascadeDelete` | `orphanRemoval` |
|---|---|---|
| Parent deleted | Referenced entities deleted | No effect |
| Reference removed from parent | No effect | Unreferenced entity deleted |
| Both enabled | Both behaviors active | Both behaviors active |

## Circular References

Morphium includes cycle detection to prevent `StackOverflowError` when serializing entities with circular `@Reference` chains:

```java
@Entity
public class Node {
    @Id
    private MorphiumId id;

    @Reference
    private Node next;
}

// A → B → A (circular)
Node a = new Node();
Node b = new Node();
a.next = b;
b.next = a;

// Morphium handles this gracefully:
// - Writer assigns IDs before serialization
// - If a cycle is detected during serialize(), objects with IDs return a minimal {_id: ...} document
// - Objects without IDs throw IllegalStateException with a clear error message
morphium.store(a);
```

**Best practices for bidirectional references:**
1. Use `lazyLoading = true` on at least one side to prevent deserialization cycles
2. Use `automaticStore = false` on one side and store that entity first
3. Both approaches can be combined:

```java
@Entity
public class Parent {
    @Reference
    private Child child;  // eager, auto-store
}

@Entity
public class Child {
    @Reference(lazyLoading = true, automaticStore = false)
    private Parent parent;  // lazy, manual store
}
```

## N+1 Query Considerations

Loading an entity with N `@Reference` fields triggers N additional queries. Mitigation strategies:

1. **Lazy loading** — defer queries until actually needed
2. **Caching** — use `@Cache` on frequently referenced entity types
3. **Embedding** — switch to `@Embedded` if the data is always loaded together
4. **Projection** — use query projections to load only the fields you need

## Comparison with JPA

| Feature | JPA | Morphium |
|---|---|---|
| Eager/Lazy loading | `@ManyToOne(fetch = LAZY)` | `@Reference(lazyLoading = true)` |
| Cascade persist | `cascade = PERSIST` | `automaticStore = true` (default) |
| Cascade delete | `cascade = REMOVE` | `cascadeDelete = true` |
| Orphan removal | `orphanRemoval = true` | `orphanRemoval = true` |
| Bidirectional | `mappedBy` + inverse side | Manual setup on both sides |
| FK constraints | Enforced by DB | Not enforced (MongoDB has no FK) |
