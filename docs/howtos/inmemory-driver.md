# InMemory Driver & MorphiumServer

> **This page has been split into two focused documents:**
> - **[InMemory Driver](../inmemory-driver.md)** - For embedded in-memory driver usage (unit tests, embedded applications)
> - **[MorphiumServer](../morphium-server.md)** - For standalone MongoDB-compatible server (CI/CD, microservices, integration testing)

## Quick Links

### For Testing and Development
If you want to run Morphium **without MongoDB** for unit tests or embedded applications:
- 👉 **[InMemory Driver Documentation](../inmemory-driver.md)**

### For Standalone Server
If you want to run a **MongoDB-compatible server** that other applications can connect to:
- 👉 **[MorphiumServer Documentation](../morphium-server.md)**

## At a Glance

**InMemory Driver:**
- Embedded in-process driver
- No network overhead
- Perfect for unit tests
- Use with: `cfg.driverSettings().setDriverName("InMemDriver")`

**MorphiumServer:**
- Standalone network server
- MongoDB wire protocol compatible
- Any MongoDB client can connect
- Run with: `java -cp morphium.jar de.caluga.morphium.server.MorphiumServer --port 27017`

---

**See the dedicated documentation pages above for complete guides, examples, and API reference.**
