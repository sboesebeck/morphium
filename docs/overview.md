# Overview

Morphium combines a high‑performance MongoDB ODM with a built‑in, database‑native messaging system.

Key features
- Integrated messaging: topic‑based queue stored in MongoDB (exclusive one‑of‑n or broadcast to all listeners)
- Own MongoDB wire‑protocol driver (no full MongoDB Java driver dependency)
- Distributed caching with cache synchronization across nodes
- Annotation‑driven mapping and validation
- Fluent query API and aggregation support
- Best practice: use field name enums in queries to avoid string typos and smooth migrations

Compatibility
- Java: 21+
- MongoDB: 5.0+

Quick start
```java
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;

// Configure
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp");
cfg.clusterSettings().addHostToSeed("localhost", 27017);

// Create client
Morphium morphium = new Morphium(cfg);
```

First entity
```java
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;

@Entity
@Cache(timeout = 30_000)
public class User {
  @Id private MorphiumId id;
  @Index private String username;
  private String email;
}
```

CRUD
```java
User u = new User();
u.setUsername("alice");
u.setEmail("alice@example.com");
morphium.store(u);

User found = morphium.createQueryFor(User.class)
    .f("username").eq("alice")
    .get();

found.setEmail("alice.new@example.com");
morphium.store(found);
morphium.delete(found);
```

Next steps
- See Developer Guide for configuration, mapping, queries, aggregation, caching, and extension points
- See Messaging for the built‑in message queue
- How‑Tos: Aggregation Examples, Caching Examples, and Cache Patterns
