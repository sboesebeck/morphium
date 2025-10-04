# Migration v5 → v6

## Requirements

- **Java 21+** (mandatory)
- **MongoDB 5.0+** for production deployments
- **BSON library**: Morphium 6 uses MongoDB's BSON library (version 4.7.1+)

## Breaking Changes

### 1. Configuration API Changes

**Old (v5):**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDatabase("mydb");
cfg.setHosts("localhost:27017");
cfg.setDriverName("PooledDriver");
```

**New (v6):**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("mydb");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver");
```

Prefer the new nested settings objects:
- `connectionSettings()` - database name, credentials, timeouts
- `clusterSettings()` - hosts, replica set configuration
- `driverSettings()` - driver selection and configuration
- `messagingSettings()` - messaging implementation and settings
- `cacheSettings()` - caching configuration
- `encryptionSettings()` - encryption keys

### 2. Driver Selection

- v5 and v6: select drivers by name via `MorphiumConfig.driverSettings().setDriverName(name)`.
- Built‑in driver names:
  - `PooledDriver` - Connection-pooled MongoDB driver (default for production)
  - `SingleMongoConnectDriver` - Single-connection driver
  - `InMemDriver` - In-memory driver for testing (no MongoDB required)
- Custom drivers must be annotated with `@de.caluga.morphium.annotations.Driver(name = "YourName")` to be discoverable.

### 3. Messaging API

**Old (v5):**
```java
Messaging m = new Messaging(morphium, 100, true);
m.setSenderId("myapp");
```

**New (v6):**
```java
// Use factory method for correct configuration
MorphiumMessaging m = morphium.createMessaging();
m.setSenderId("myapp");
m.start();
m.addListenerForTopic("topic", (mm, msg) -> {
    // process message
    return null;
});
```

- **Always use `Morphium.createMessaging()`** so configuration and implementation selection happen correctly
- The messaging implementation is chosen by name from `MorphiumConfig.messagingSettings().getMessagingImplementation()`
- Built-in implementations:
  - `StandardMessaging` (default) - Single-collection messaging
  - `MultiCollectionMessaging` - Multi-collection messaging for high-volume scenarios
- Custom implementations must be annotated with `@de.caluga.morphium.annotations.Messaging(name = "YourName")`

### 4. InMemoryDriver Improvements (v6)

The InMemoryDriver received major enhancements in v6.0:

#### Change Streams
- **Full change stream support** with document snapshots
- **Proper event isolation** preventing dirty reads
- **Database-scoped driver sharing** with reference counting
- Multiple Morphium instances can share the same InMemoryDriver when using the same database name

#### Message Queue Testing
```java
// Multiple messaging instances can now share InMemoryDriver correctly
MorphiumMessaging msg1 = morphium1.createMessaging();
MorphiumMessaging msg2 = morphium2.createMessaging();
// Both will receive change stream events from the shared driver
```

#### Known Limitations
- No replica set simulation (single-instance only)
- No sharding support
- Limited geospatial operations compared to MongoDB
- See `docs/howtos/inmemory-driver.md` for detailed feature coverage

## New Features in v6

### 1. Virtual Threads Support
Morphium 6 uses Java 21 virtual threads for:
- Change stream event dispatching
- Async operation handling
- Lightweight concurrent processing

### 2. Enhanced Configuration
- URI-based configuration: `MONGODB_URI` environment variable
- System properties: `-Dmorphium.driver=inmem`
- Properties file: `morphium-test.properties`

### 3. Test Infrastructure
- Tag-based test organization (`@Tag("core")`, `@Tag("messaging")`, `@Tag("inmemory")`)
- Improved test runner: `./runtests.sh` with retry logic and filtering
- Better isolation between tests with proper driver lifecycle management

## Migration Checklist

- [ ] Update to Java 21+
- [ ] Update pom.xml dependency to `6.0.0` or higher
- [ ] Replace flat config setters with nested settings objects
- [ ] Change messaging instantiation to `morphium.createMessaging()`
- [ ] Review custom driver/messaging implementations for annotation requirements
- [ ] Update test infrastructure to use tags if using parameterized tests
- [ ] Test with InMemoryDriver to verify change stream handling

## JMS Support

- The JMS classes provided are experimental/incomplete and should not be relied upon for full JMS compatibility
- May be completed in upcoming versions

## Getting Help

- Check `docs/troubleshooting-guide.md` for common issues
- Review `docs/howtos/` for specific use cases
- Join our Slack: https://join.slack.com/t/team-morphium/shared_invite/...
