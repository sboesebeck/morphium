# Basic Setup

Connect to a local MongoDB and configure Morphium.

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp");
cfg.clusterSettings().addHostToSeed("localhost", 27017);

Morphium morphium = new Morphium(cfg);
```

Replica set example
```java
cfg.clusterSettings().addHostToSeed("mongo1", 27017);
cfg.clusterSettings().addHostToSeed("mongo2", 27017);
cfg.clusterSettings().addHostToSeed("mongo3", 27017);
```

Authentication
```java
cfg.authSettings().setMongoLogin("user");
cfg.authSettings().setMongoPassword("secret");
```

Driver selection (optional)
```java
cfg.driverSettings().setDriverName("PooledDriver"); // default
```

