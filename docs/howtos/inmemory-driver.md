# InMemory Driver (Testing)

Use the in‑memory driver to run tests without a MongoDB instance.

Set the driver name to `InMemDriver`:
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("testdb");
cfg.driverSettings().setDriverName("InMemDriver");

Morphium morphium = new Morphium(cfg);
```

Notes
- Intended for testing; not all MongoDB features are available
- No authentication
- Supports querying, aggregation, partial updates, and messaging

