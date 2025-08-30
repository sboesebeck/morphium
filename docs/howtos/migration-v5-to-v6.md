# Migration v5 → v6

Requirements
- Java 21+
- MongoDB 5.0+

Driver selection
- v5: often used class names
- v6: use driver names (e.g., `PooledDriver`, `SingleMongoConnectDriver`, `InMemDriver`)

Messaging API
- Register listeners by topic:
```java
StdMessaging m = new StdMessaging();
m.init(morphium);
m.addListenerForTopic("topic", (mm, msg) -> null);
```

Configuration accessors
- Prefer nested settings on `MorphiumConfig` (e.g., `connectionSettings()`, `clusterSettings()`, `driverSettings()`, `messagingSettings()`) over deprecated top‑level setters.

JMS
- The JMS classes provided are experimental/incomplete and should not be relied upon for full JMS compatibility.

