# InMemory Driver (Testing)

The InMemoryDriver provides a fully in-memory MongoDB-compatible database for testing without requiring a MongoDB instance. It's perfect for unit tests, CI/CD pipelines, and development environments.

## Quick Start

Set the driver name to `InMemDriver`:
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("testdb");
cfg.driverSettings().setDriverName("InMemDriver");

Morphium morphium = new Morphium(cfg);
```

Or use environment variable:
```bash
export MORPHIUM_DRIVER=inmem
mvn test
```

Or system property:
```bash
mvn test -Dmorphium.driver=inmem
```

## Supported Features (v6.0)

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
- ⚠️ **Advanced Stages**: $lookup, $unwind, $facet (limited support)
- ❌ **MapReduce**: Not yet implemented

### Change Streams (v6.0)
- ✅ **Event Types**: insert, update, delete, drop operations
- ✅ **Document Snapshots**: Immutable snapshots prevent dirty reads
- ✅ **Pipeline Filtering**: Filter events with aggregation pipelines
- ✅ **Full Document Support**: Access complete document in change events
- ✅ **Database-scoped Sharing**: Multiple Morphium instances share driver per database

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

**Database-scoped Driver Sharing**
```java
// Multiple Morphium instances sharing the same database will share the driver
MorphiumConfig cfg1 = new MorphiumConfig();
cfg1.connectionSettings().setDatabase("testdb");
cfg1.driverSettings().setDriverName("InMemDriver");
Morphium m1 = new Morphium(cfg1);

MorphiumConfig cfg2 = new MorphiumConfig();
cfg2.connectionSettings().setDatabase("testdb");  // same database
cfg2.driverSettings().setDriverName("InMemDriver");
Morphium m2 = new Morphium(cfg2);

// m1 and m2 share the same InMemoryDriver instance
// Change streams work correctly across both instances
// Driver is only closed when the last Morphium instance closes
```

**Reference Counting**
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
- ❌ **MapReduce**: JavaScript engine integration pending

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
    cfg1.connectionSettings().setDatabase(dbName);

    MorphiumConfig cfg2 = new MorphiumConfig();
    cfg2.driverSettings().setDriverName("InMemDriver");
    cfg2.connectionSettings().setDatabase(dbName);

    try (Morphium m1 = new Morphium(cfg1);
         Morphium m2 = new Morphium(cfg2)) {

        // Both share the same driver
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

