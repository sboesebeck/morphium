# Migration v5 → v6

Requirements

- Java 21+
- MongoDB 5.0+

Driver selection

- v5 and v6: select drivers by name via `MorphiumConfig.driverSettings().setDriverName(name)`.
- Built‑in names include `PooledDriver`, `SingleMongoConnectDriver`, and `InMemDriver` (names already existed in v5).
- Custom drivers must be annotated with `@de.caluga.morphium.annotations.Driver(name = "YourName")` to be discoverable.

Messaging API
- Instantiate via `Morphium.createMessaging()` so configuration and implementation selection happen correctly:
```java
import de.caluga.morphium.messaging.MorphiumMessaging;

MorphiumMessaging m = morphium.createMessaging();  // uses MorphiumConfig.messagingSettings()
m.start();
m.addListenerForTopic("topic", (mm, msg) -> null);
```

- The messaging implementation is chosen by name from `MorphiumConfig.messagingSettings().getMessagingImplementation()`.
  Implementations must be annotated with `@de.caluga.morphium.annotations.Messaging(name = "YourName")`.

Configuration accessors

- Prefer nested settings on `MorphiumConfig` (e.g., `connectionSettings()`, `clusterSettings()`, `driverSettings()`, `messagingSettings()`) over deprecated top‑level setters.

JMS

- The JMS classes provided are experimental/incomplete and should not be relied upon for full JMS compatibility. Might be completed in upcoming versions though.
