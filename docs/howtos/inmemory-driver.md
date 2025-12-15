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

### Core Operations
- ✅ **CRUD Operations**: insert, find, update, delete, upsert
- ✅ **Queries**: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex
- ✅ **Logical Operators**: $and, $or, $not, $nor
- ✅ **Array Operators**: $elemMatch, $all, $size
- ✅ **Projections**: Field inclusion/exclusion, nested fields
- ✅ **Sorting & Pagination**: sort(), skip(), limit()

### Aggregation Pipeline
- ✅ **Basic Stages**: $match, $group, $sort, $limit, $skip, $project
- ✅ **Group Operators**: $sum, $avg, $min, $max, $first, $last, $push, $addToSet
- ✅ **MapReduce**: Full JavaScript-based MapReduce with GraalJS engine
- ⚠️ **Advanced Stages**: $lookup, $unwind, $facet (limited support)

### Change Streams (v6.0)
- ✅ **Event Types**: insert, update, delete, drop operations
- ✅ **Document Snapshots**: Immutable snapshots prevent dirty reads
- ✅ **Pipeline Filtering**: Filter events with aggregation pipelines
- ✅ **Full Document Support**: Access complete document in change events
- ✅ **Database-scoped Sharing**: Optional driver sharing for multiple Morphium instances (opt-in)

### Messaging System (v6.0)
- ✅ **StandardMessaging**: Single-collection messaging with change streams
- ✅ **MultiCollectionMessaging**: Multi-collection messaging
- ✅ **Exclusive Messages**: Single-consumer message processing
- ✅ **Broadcast Messages**: Multi-consumer message distribution
- ✅ **Message Locking**: Proper lock collection support

### Indexes
- ✅ **Single Field Indexes**: Basic indexing support
- ⚠️ **Compound Indexes**: Limited support
- ❌ **Text Indexes**: Not fully implemented
- ❌ **Geospatial Indexes**: Limited geospatial support

### Transactions
- ✅ **Basic Transactions**: start, commit, abort (single-instance)
- ❌ **Multi-document ACID**: Limited to single instance
- ❌ **Distributed Transactions**: No replica set support

## V6.0 Improvements

### Change Stream Enhancements
The v6.0 release significantly improved change stream reliability:

**Deep Copy Snapshots**
```java
// Documents are deep-copied before change stream events are dispatched
// This prevents dirty reads where documents are modified before callbacks execute
morphium.watch(UncachedObject.class, evt -> {
    // evt.getFullDocument() contains an immutable snapshot
    // Safe to process without worrying about concurrent modifications
});
```

**Database-scoped Driver Sharing (Opt-in)**

By default, each Morphium instance gets its own separate InMemoryDriver. To enable sharing between instances with the same database name, use `setInMemorySharedDatabases(true)`:

```java
// Enable driver sharing for multiple Morphium instances
MorphiumConfig cfg1 = new MorphiumConfig();
cfg1.connectionSettings().setDatabase("testdb");
cfg1.driverSettings().setDriverName("InMemDriver");
cfg1.driverSettings().setInMemorySharedDatabases(true);  // Enable sharing
Morphium m1 = new Morphium(cfg1);

MorphiumConfig cfg2 = new MorphiumConfig();
cfg2.connectionSettings().setDatabase("testdb");  // same database
cfg2.driverSettings().setDriverName("InMemDriver");
cfg2.driverSettings().setInMemorySharedDatabases(true);  // Enable sharing
Morphium m2 = new Morphium(cfg2);

// m1 and m2 share the same InMemoryDriver instance
// Change streams work correctly across both instances
// Driver is only closed when the last Morphium instance closes
```

**Reference Counting (when sharing enabled)**
- Automatic reference counting prevents premature driver shutdown
- Each Morphium instance increments the ref count on creation
- Driver shuts down only when ref count reaches zero
- Solves issues with tests that create multiple Morphium instances

### Messaging Improvements

**No More Re-reads**
```java
// v5: messaging layer re-read documents from change stream events
// v6: uses evt.getFullDocument() directly - more efficient, no dirty reads
MorphiumMessaging msg = morphium.createMessaging();
msg.addListenerForTopic("events", (m, message) -> {
    // message is from the immutable snapshot, not a re-read
    return null;
});
```

**Better Multi-Instance Support**
```java
// Tests with multiple messaging instances now work correctly
Morphium m1 = new Morphium(cfg);
Morphium m2 = new Morphium(cfg);  // same database

MorphiumMessaging msg1 = m1.createMessaging();
MorphiumMessaging msg2 = m2.createMessaging();

// Both receive change stream events correctly
// Exclusive messages work as expected
// Broadcast messages delivered to all listeners
```

## MapReduce Support

The InMemory driver includes full MapReduce support using the GraalJS JavaScript engine.

### Basic Example

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.driverSettings().setDriverName("InMemDriver");
cfg.connectionSettings().setDatabase("testdb");

try (Morphium morphium = new Morphium(cfg)) {
    // Insert sample data
    for (int i = 0; i < 100; i++) {
        MyEntity entity = new MyEntity();
        entity.setCategory(i % 5);  // 5 categories
        entity.setValue(i);
        morphium.store(entity);
    }

    // Map function (JavaScript)
    String mapFunction = """
        function() {
            emit(this.category, this.value);
        }
        """;

    // Reduce function (JavaScript)
    String reduceFunction = """
        function(key, values) {
            return values.reduce((sum, val) => sum + val, 0);
        }
        """;

    // Execute MapReduce
    List<Map<String, Object>> results = morphium.mapReduce(
        MyEntity.class,
        mapFunction,
        reduceFunction
    );

    // Process results
    results.forEach(r ->
        System.out.println("Category: " + r.get("_id") + ", Total: " + r.get("value"))
    );
}
```

### JavaScript Engine
- **Engine**: GraalJS (modern ES6+ compatible)
- **Available**: `emit(key, value)` function and all standard JavaScript built-ins
- **Performance**: Fast in-memory execution, single-threaded

For more MapReduce examples, see the [InMemory Driver](../inmemory-driver.md) documentation.

## Limitations

### Not Supported
- ❌ **Replica Sets**: No replica set simulation
- ❌ **Sharding**: No shard key or distributed queries
- ❌ **Full Text Search**: Limited $text operator support
- ❌ **Advanced Geospatial**: Basic $near/$geoWithin only
- ❌ **GridFS**: No file storage support
- ❌ **Time Series Collections**: Not implemented
- ❌ **Authentication**: No user/role management
- ❌ **$lookup Joins**: Not yet implemented

### Performance Considerations
- **Memory Usage**: All data stored in memory
- **No Persistence**: Data lost when driver closes
- **Concurrency**: Uses ReadWriteLock for thread safety
- **Index Performance**: Limited compared to MongoDB's B-tree indexes

## Testing Strategies

### Unit Tests
```java
@Test
public void testWithInMemory() {
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.driverSettings().setDriverName("InMemDriver");
    cfg.connectionSettings().setDatabase("unittest");

    try (Morphium morphium = new Morphium(cfg)) {
        // Test code here
        // No MongoDB required!
    }
}
```

### Shared Driver Tests
```java
@Test
public void testMultipleInstances() {
    String dbName = "shareddb";

    MorphiumConfig cfg1 = new MorphiumConfig();
    cfg1.driverSettings().setDriverName("InMemDriver");
    cfg1.driverSettings().setInMemorySharedDatabases(true);  // Enable sharing
    cfg1.connectionSettings().setDatabase(dbName);

    MorphiumConfig cfg2 = new MorphiumConfig();
    cfg2.driverSettings().setDriverName("InMemDriver");
    cfg2.driverSettings().setInMemorySharedDatabases(true);  // Enable sharing
    cfg2.connectionSettings().setDatabase(dbName);

    try (Morphium m1 = new Morphium(cfg1);
         Morphium m2 = new Morphium(cfg2)) {

        // Both share the same driver (sharing enabled)
        // Write with m1, read with m2
        m1.store(new MyEntity("test"));
        MyEntity found = m2.findById(MyEntity.class, id);

        // Works correctly!
    }
}
```

### Messaging Tests
```java
@Test
public void testMessaging() throws Exception {
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.driverSettings().setDriverName("InMemDriver");
    cfg.connectionSettings().setDatabase("msgtest");

    try (Morphium morphium = new Morphium(cfg)) {
        MorphiumMessaging sender = morphium.createMessaging();
        MorphiumMessaging receiver = morphium.createMessaging();

        AtomicInteger count = new AtomicInteger(0);

        receiver.addListenerForTopic("test", (m, msg) -> {
            count.incrementAndGet();
            return null;
        });

        sender.start();
        receiver.start();

        sender.sendMessage(new Msg("test", "Hello", "World", 30000));

        Thread.sleep(500);
        assertEquals(1, count.get());
    }
}
```

## Monitoring

**Built-in Status Monitoring**: When using messaging with InMemory driver, all instances automatically respond to `morphium_status` queries. This provides JVM, messaging, and driver metrics without any setup.

```java
MorphiumMessaging sender = morphium.createMessaging();
sender.start();

// Query status from all instances
List<Msg> responses = sender.sendAndAwaitAnswers(
    new Msg(sender.getStatusInfoListenerName(), "status", "ALL"),
    5, 2000
);

// Check metrics
for (Msg r : responses) {
    Map<String, Object> stats = r.getMapValue();
    System.out.println("Heap: " + stats.get("jvm.heap.used"));
    System.out.println("Processing: " + stats.get("messaging.processing"));
}
```

See **[Messaging - Built-in Status Monitoring](../messaging.md#built-in-status-monitoring)** for complete documentation.

## Best Practices

1. **Use for Unit Tests Only**: Not intended for production
2. **Separate Database Names**: Different test classes should use different database names to avoid interference
3. **Clean Up**: Use try-with-resources to ensure proper cleanup
4. **Test Against Real MongoDB**: Always verify behavior against actual MongoDB before production
5. **Watch Memory Usage**: Large datasets can consume significant memory

## Troubleshooting

### Issue: Change streams not working
**Solution**: Ensure you're using v6.0+ with the deep copy snapshot fix

### Issue: Messages not received by all listeners
**Solution**: Use database-scoped sharing by ensuring all Morphium instances use the same database name

### Issue: NullPointerException in insert()
**Solution**: Upgrade to v6.0+ which includes index data structure initialization fix

### Issue: Driver shutdown too early
**Solution**: v6.0+ includes reference counting to prevent premature shutdown

## See Also

- `docs/howtos/messaging-implementations.md` - Messaging patterns
- `docs/testing-guide.md` - Testing strategies
- `docs/howtos/migration-v5-to-v6.md` - Migration guide

**See the dedicated documentation pages above for complete guides, examples, and API reference.**
