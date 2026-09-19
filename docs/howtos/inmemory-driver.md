# InMemory Driver & PoppyDB

This page has been split into two focused documents:

- **[InMemory Driver](../inmemory-driver.md)** — the embedded, in-process driver for unit
  tests and embedded applications (`cfg.driverSettings().setDriverName("InMemDriver")`):
  capabilities, caveats (including the [on-disk format gap](../inmemory-driver.md#not-suitable-for-on-disk-format-tests-336)),
  and testing strategies.
- **[PoppyDB](../poppydb.md)** — the standalone, MongoDB-wire-compatible server for CI/CD,
  microservices and integration testing (`java -jar poppydb-cli.jar --port 27017`).

See also [Messaging Implementations](messaging-implementations.md) and
[Migration v5→v6](migration-v5-to-v6.md).
