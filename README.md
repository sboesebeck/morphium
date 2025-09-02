# morphium

**Want to contribute? Every help is welcome... just contact us**

Documentation
- Overview: see `docs/overview.md`
- Developer Guide: see `docs/developer-guide.md`
- Messaging: see `docs/messaging.md`
- How‑Tos: `docs/howtos/` (e.g., migration v5→v6)

Also have a look at our blog at https://caluga.de where we post interesting Morphium code and examples.

Morphium - Java Messaging (and POJO Mapper) based on MongoDB

Morphium is a POJO Object mapper for Accessing Mongodb. Some of the main Features of Morphium:

- actively developed
- used by a number of projects (including holidayinsider.com and holidays.hrs.de/, simplesystem.com, genios.de)
- Transparent access to MongoDB
- Transparent declarative (Annotation based) caching
- Annotation based definition of mappings
- Cluster Awareness
- Cache synchronization between cluster nodes
- Asynchronous and Buffered write access
- Messaging (or better MessageQueueing - for more information about Messaging via morphium, read this blog entry: https://caluga.de/v/2018/5/5/mongodb_messaging_via_morphium)
- support for Transactions (as mongodb does)
- Since V3.2 we put a lot of effort in utelising the oplog listening feature and with mognodb 4.0 the `watch` feature. This gives us kind of push notification on changes to collections or databases. This is used both in Messaging and for Cache Synchronization in Clustered environments! (since V4.0 only)
- fluent interface for querying mongodb
- support for the MongoDB aggregator framework
- support for complex queries
- support inheritance and polymorphism
- support for javax.validation annotations
- lifecycle methods of POJOs
- Support for References, including lazy loaded references
- Support for partial updated objects (when writing, only the changes of the object are transferred)
- Almost any operation Morphium provides is async capable. That means, if you pass it an `AsyncOperationListener` as argument, you won't get a batch now, but after the async operation finished via the callback

for questions and feature requests / bug reports also have a look at the google group
join us on _slack_ [link](https://join.slack.com/t/team-morphium/shared_invite/enQtMjgwODMzMzEzMTU5LTA1MjdmZmM5YTM3NjRmZTE2ZGE4NDllYTA0NTUzYjU2MzkxZTJhODlmZGQ2MThjMGY0NmRkMWE1NDE2YmQxYjI)

# Compatibility

- Morphium v6 requires Java 21+ and MongoDB 5.0+.
- Morphium provides its own MongoDB wire‑protocol driver; only the BSON package is required as an extra dependency.

Maven dependencies
```
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>[6.0.0,)</version>
  <!-- use the latest v6 release -->
  </dependency>
<dependency>
  <groupId>org.mongodb</groupId>
  <artifactId>bson</artifactId>
  <version>4.7.1</version>
</dependency>
```

Upgrading from v5? See `docs/howtos/migration-v5-to-v6.md`.

# Quick Start

before accessing mongo via Morphium, you need to configure Morphium. this is done by preparing a MorphiumConfig Object:

```java
  MorphiumConfig cfg = new MorphiumConfig();
  cfg.connectionSettings().setDatabase("testdb");
  cfg.clusterSettings().addHostToSeed("localhost", 27017);
```

The seed of hosts is at least one node from the replicaset. Actually, only one is needed, as the configuration of the replicaset is read by the driver to know which machines are available. Attention: if it happens that exactly the hosts of the replicaset you defined here are not available, morphium won't start and you will end up with an exception.

you can also configure Morphium using properties: `new MorphiumConfig(properties);` or a json-String: `MorphiumConfig cfg = MorphiumConfig.createFromJson(json);`

After that, you just need to instantiate Morphium:

```
  Morphium m=new Morphium(cfg);
```

There are some convenience constructors available since _V2.2.23_ which make your life a bit easier:

```
  Morphium m=new Morphium("localhost","test-db");
```

this creates a morphium instance with most default settings, connecting to localhost, standard port 27017 and using database `test-db`

if necessary, it's of course possible to specify the port to connect to:

```
  Morphium m=new Morphium("localhost:27019","test-db");
  Morphium n=new Morphium("localhost",27020,"test-db");
```

then you are good to go:

```java
  Query<MyEntity> q=m.createQueryFor(MyEntity.class).f("a_field").eq("a id");
  List<MyEntity> lst=q.asList();
  MyEntity ent=q.get();
  ...
  m.store(ent);
```

Defining an Entity is quite simple as well:

```java
  @Entity(translateCamelCase = true)
  @Cache
  public class MyEntity {
    @Id
    private MorphiumId myId;

    private String aField;

    private EmbeddedObject emb;

    @Reference
    private MyEntity otherEntity;

   ...
  }

  @Embedded
  public class EmbeddedObject {
   ...
  }
```

All Entities need an ID field. This field can by any type you like. If it is not `ObjectID` or `MorphiumID, you need to create the id yourself.

- Before Morphium 3.x: If the type is `org.bson.types.ObjectId`, it will be created by mongo, if not - you need to take care of that.
- Since Morphium 3: To encapsulate dependencies better, morphium is not relying on mongodb code directly. Hence in your code, you do not use `ObjectID` anymore, but MorphiumID.
  These are more or less source compatible, migration should be easy. But the rule from above applies here too, if you use MorphiumID, you do not need to create the ID, Mongo is doing it.

Only entities can be referenced, as you need an ID for that. The type of the ID is not important.
You can also use Maps, Lists or Arrays, all may also include other Entities or Embedded types.
``

## Use enum instead of strings for queries

As using strings to query your object might be a bit error prone, you also can use enums instead of field name strings:

```java
   Query<MyEntity> q=m.createQueryFor(MyEntity.class).f(MyEntity.Fields.aField).eq("a id");
```

of course, these enums need to be created. have a look at https://github.com/sboesebeck/intelliJGenPropertyEnumsPlugin for a plugin for generating those automatically
in our example, the batch would look like this:

```java
  @Entity(translateCamelCase = true)
  @Cache
  public class MyEntity {
    @Id
    private MorphiumId myId;

    private String aField;

    private EmbeddedObject emb;

    @Reference
    private MyEntity otherEntity;
    ...
    public enum Fields { myId, aField, emb, otherEntity }

  }

  @Embedded
  public class EmbeddedObject {
   ...
  }
```

This is a very short glance at all the features of Morphium!

For more information, see the documentation in the `docs/` folder.

Have fun,

Stephan


# InMemoryDriver

Morphium does have support for an `InMemoryDriver` since V4.2.0 - This driver is
basically not connecting to a mongodb but does mock a mongo in memory. This is
for testing purposes only.
Especially with V5.0 there was a lot of effort put in to make the
InMemoryDriver more or less feature complete. Most operations a mongoDB
offers, is also implemented in the InMemDriver.
To use the inMemDriver you only need to set the DriverName directly in
`MorphiumConfig` via `cfg.driverSettings().setDriverName("InMemDriver")` or in
properties like `driver_name=InMemDriver'`.

There is a test-version of a listening server for this driver - so you can try
out using standard tools. 

## Caveats
the InMemoryDriver is for testing purposes only and does not support all
features, a mongodb offers! It lacks support for most of the JavaScript
functionalities, geospacial queries and some others. Authentication is not
supported! 
Use it with caution!


But it does support Aggregation, querying, Messaging, Partial Updates.

# Aggregation

## Count

Counting is quite easy, call countAll() on any given Query-instance. Please keep in Mind: countAll() does not take limit() or skip() into account, always returns the number of all possible matches to the query.

## Distinct values

Morphium supports distinct queries (since 1.4.07) and allows you to get a list of values existing in the query result. Have a look, how to use it:

```java
List<UncachedObject> lst = new ArrayList<UncachedObject>();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i % 3);
            uc.setValue("Value " + (i % 2));
            lst.add(uc);
        }
        MorphiumSingleton.get().storeList(lst);

        List values = MorphiumSingleton.get().distinct("counter", UncachedObject.class);
        assert (values.size() == 3) : "Size wrong: " + values.size();
        for (Object o : values) {
            log.info("counter: " + o.toString());
        }
        values = MorphiumSingleton.get().distinct("value", UncachedObject.class);
        assert (values.size() == 2) : "Size wrong: " + values.size();
        for (Object o : values) {
            log.info("Value: " + o.toString());
        }
```

The output would look like:

```
19:03:17,211 INFO  [main] DistinctGroupTest: counter: 0
19:03:17,211 INFO  [main] DistinctGroupTest: counter: 1
19:03:17,211 INFO  [main] DistinctGroupTest: counter: 2
19:03:17,213 INFO  [main] DistinctGroupTest: Value: Value 0
19:03:17,213 INFO  [main] DistinctGroupTest: Value: Value 1
```

Here the call to "distinct" returns a list ov values, which (of course) might be of any type. Those values are not unmarshalled, which means, if this would contain a sub-document, you will get a list of BasicDBObject.

## Group Function

Morphium has support for MongoDB's group-Function (similar to SQL group by). Here is an example:

```java
        HashMap<String, Object> initial = new HashMap<String, Object>();
        initial.put("count", 0);
        initial.put("sum", 0);
        DBObject ret = MorphiumSingleton.get().group(MorphiumSingleton.get().createQueryFor(UncachedObject.class), initial,
                "data.count++; data.sum+=obj.counter;", "data.avg=data.sum/data.count;");
        log.info("got DBObject: " + ret);
```

Producing output:

```
19:05:19,517 INFO  [main] DistinctGroupTest: got DBObject: [ { "count" : 100.0 , "sum" : 5050.0 , "avg" : 50.5}]
```

## some Explanations:

parameter query is the query to execute, might be an empty one for the whole collection
initial is a Map containing the data object that is manipulated by the javascript code in 'jsReduce' and 'jsFinalize'
jsReduce is the call to reduce the data to what is needed, here count all objects and sum up the values. If you did not define the reduce function yourself in the given string, a function (obj,data) { and the closing brackets are added
jsFinalize: Javascript code to finalize the data, if no function is specified in the string, function(data) {and the closing brackets are added. Here: finalize calculates the avg of all values
you might also add fields to group by. Those might either all be not prefixed or prefixed with a - to have it not grouped by that
the return value is a list of DBObject, containing at least the values from the initialMap given in the call!

**Attention** this might cause heavy load on the mongo if the correct indices are missing.

# Support for mongodb aggregation framework

With V2.2.0 10gen introduces the new aggregation framework to the mongodb. Very useful for calculating statistics and such. For mor informrmation see Mongodb Aggregation Docu.

Since V1.5.0 morphium has support for this fexible aggregation framework. Consider this the first basic API, there will be some improvements, as soon as we know in what direction to improve the API.

For now, have a look at this example:

```java
public class Aggregation extends MongoTest {
    @Test
    public void aggregatorTest() throws Exception {
        createUncachedObjects(1000);

        Aggregator<UncachedObject, Aggregate> a = MorphiumSingleton.get().createAggregator(UncachedObject.class, Aggregate.class);
        assertNotNull(a.getResultType());;
        //reduce input amount of data by reducing columns
        a = a.project("counter");
        //filter it more
        a = a.match(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").gt(100));
        //sorting, necessary for $first/$last
        a = a.sort("counter");
        //limit input data rows
        a = a.limit(15);
        //group by - in this case, calculate considerin ALL data, as _id does not change
        a = a.group("all").avg("schnitt", "$counter").sum("summe", "$counter").sum("anz", 1).last("letzter", "$counter").first("erster", "$counter").end();
        //project the result into the desired structure (rename fields and such)
        a = a.project(new BasicDBObject("summe", 1).append("anzahl", "$anz").append("schnitt", 1).append("last", "$letzter").append("first", "$erster"));

        List<DBObject> obj = a.toAggregationList();
        for (DBObject o : obj) {
            log.info("Object: " + o.toString());
        }
        List<Aggregate> lst = a.aggregate();
        assert (lst.size() == 1) : "Size wrong: " + lst.size();
        log.info("Summe:   " + lst.get(0).getSumme());
        log.info("Schnitt: " + lst.get(0).getSchnitt());
        log.info("Last:    " + lst.get(0).getLast());
        log.info("First:   " + lst.get(0).getFirst());
        log.info("Anzahl:  " + lst.get(0).getAnzahl());


        assert (lst.get(0).getAnzahl() == 15) : "Anzahl found: " + lst.get(0).getAnzahl();

    }

    @Test
    public void aggregationTestcompare() throws Exception {
        log.info("Preparing data");
        createUncachedObjects(100000);
        log.info("done... starting");
        long start = System.currentTimeMillis();
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        HashMap<Integer, Integer> sum = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> anz = new HashMap<Integer, Integer>();
        q = q.sort("counter");

        for (UncachedObject u : q.asList()) {
            int v = u.getCounter() % 3;
            if (sum.get(v) == null) {
                sum.put(v, u.getCounter());
            } else {
                sum.put(v, sum.get(v).intValue() + v);
            }
            if (anz.get(v) == null) {
                anz.put(v, 1);
            } else {
                anz.put(v, anz.get(v).intValue() + 1);
            }

        }
        for (Integer v : sum.keySet()) {
            log.info("ID: " + v);
            log.info("  anz: " + anz.get(v));
            log.info("  sum: " + sum.get(v));
            log.info("  avg: " + (sum.get(v) / anz.get(v)));
        }
        long dur = System.currentTimeMillis() - start;

        log.info("Query took " + dur + "ms");

        log.info("Starting test with Aggregation:");
        start = System.currentTimeMillis();
        Aggregator<UncachedObject, Aggregate> a = MorphiumSingleton.get().createAggregator(UncachedObject.class, Aggregate.class);
        assertNotNull(a.getResultType());;
        BasicDBList params = new BasicDBList();
        params.add("$counter");
        params.add(3);
        BasicDBObject db = new BasicDBObject("$mod", params);
        a = a.sort("$counter");
        a = a.group(db).sum("summe", "$counter").sum("anzahl", 1).avg("schnitt", "$counter").end();
        List<DBObject> obj = a.toAggregationList();
        List<Aggregate> lst = a.aggregate();
        assert (lst.size() == 3);
        for (Aggregate ag : lst) {
            log.info("ID: " + ag.getTheGeneratedId());
            log.info(" sum:" + ag.getSumme());
            log.info(" anz:" + ag.getAnzahl());
            log.info(" avg:" + ag.getSchnitt());
        }
        dur = System.currentTimeMillis() - start;
        log.info("Aggregation took " + dur + "ms");
    }

    @Embedded
    public static class Aggregate {
        private double schnitt;
        private long summe;
        private int last;
        private int first;
        private int anzahl;

        @Property(fieldName = "_id")
        private String theGeneratedId;

        public int getAnzahl() {
            return anzahl;
        }

        public void setAnzahl(int anzahl) {
            this.anzahl = anzahl;
        }

        public int getLast() {
            return last;
        }

        public void setLast(int last) {
            this.last = last;
        }

        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public double getSchnitt() {
            return schnitt;
        }

        public void setSchnitt(double schnitt) {
            this.schnitt = schnitt;
        }

        public long getSumme() {
            return summe;
        }

        public void setSumme(long summe) {
            this.summe = summe;
        }

        public String getTheGeneratedId() {
            return theGeneratedId;
        }

        public void setTheGeneratedId(String theGeneratedId) {
            this.theGeneratedId = theGeneratedId;
        }
    }


}
```

the second test case compares performance using the "usual" approach, reading all data, doing calculations with the aggregation framework approach. On my machine, the aggregation framework is about 8 times faster and produces way less load on mongo.

# Annotation Inheritence

By default, Java does not support the inheritence of Annotations. This is ok in most cases, but in the case of entities it's a bugger. I added inheritence to morphium to be able to build flexible data structures and store them to mongo.

## Implementation

Well, it's quite easy, actually ;-) The algorithm for getting the inherited annotations looks as follows (simplified)

Take the annotations from the current class, if found, return it
Take the superclass, if superclass is "Object" return null
if there is the annotation to look for, return it
continue with step 1
This way, all annotations in the hierarchy are taken into account and the most recent one is taken. You can always change the annotations when subclassing, although you cannot "erase" them (which means, if you inherit from an entity, it's always an entity). For Example:

```java
   @Entity
   @NoCache
   public class Person {
      @Id
      private ObjectId id;
     ....
   }
```

And the subclass:

```java
   @Cache(writeCache=true, readCache=false)
   public class Parent {
      @Reference
      private List<Person> parentFrom;
      ...
   }
```

Please keep in mind, that unless specified otherwise, the classname will be taken as the name for your collection. Also, be sure to store your classname in the collection (set polymorph=true in @Entity annotation) if you want to store them in one collection.

All writer implementation support asynchronous calls like

```java
   public <T> void store(List<T> lst, AsyncOperationCallback<T> callback);
```

if callback==null the method call should be synchronous... If callback!=null do the call to mongo asynchronous in background. Usually, you specify the default behavior in your class definition:

```java
  @Entity
  @AsyncWrites
  public class EntityType {
   ...
  }
```

All write operations to this type will be asynchronous! (synchronous call is not possible in this case!).

Asynchronous calls are also possible for Queries, you can call q.asList(callback) if you want to have this query be executed in background.

# Difference asynchronous write / write buffer

Asynchronous calls will be issued at once to the mongoDb but the calling thread will not have to wait. It will be executed in Background. the `@WriteBuffer` annotation specifies a write buffer for this type (you can specify the size etc if you like). All writes will be held temporarily in ram until time frame is reached or the number of objects in write buffer exceeds the maximum you specified (0 means no maximum). Attention if you shut down the Java VM during that time, those entries will be lost. Please only use that for logging or "not so important" data. specifying a write buffer four you entitiy is quite easy:

```java
  @Entity
  @WriteBuffer(size=1000, timeout=5000)
  public class MyBufferedLog {
  ....
  }
```

This means, all write access to this type will be stored for 5 seconds or 1000 entries, whichever occurs first. If you want to specify a different behavior when the maximum number of entries is reached, you can specify a strategy:

`WRITE_NEW`: write newest entry (synchronous and not add to buffer)

`WRITE_OLD`: write some old entries (and remove from buffer)

`DEL_OLD`: delete old entries from buffer - oldest elements won't be written to Mongo!

`IGNORE_NEW`: just ignore incoming - newest elements WILL NOT BE WRITTEN!

`JUST_WARN`: increase buffer and warn about it

# Authentication

In Mongo until V 2.4 authentication and user privileges were not really existent. With 2.4, roles are introduces which might make it a bit more complicated to get things working.

Morphum and authentication
Morphium supports authentication, of course, but only once. So usually you have an application user, which connects to database. Login to mongo is configured as follows:

```java
    MorphiumConfig cfg=new Morpiumconfig(...);
    ...
    cfg.setMongoLogin("tst");
    cfg.setMongoPassword("tst");
```

This user usually needs to have read/Write access to the database. If you want your indices to be created automatically by you, this user also needs to have the role dbAdmin for the corresponding database. If you use morphium with a replicase of mongo nodes, morphium needs to be able to get access to local database and get the replicaset status. In order to do so, either the mongo user needs to get additional roles (clusterAdmin and read to local db), or you specify a special user for that task, which has excactly those roles. Morphium authenticates with that different user for accessing replicaSet status (and only for getting the replicaset status) and is convigured very similar to the normal login:

```java
     cfg.setMongoAdminUser("adm");
     cfg.setMongoAdminPwd("adm");
```

# Corresponding MongoD Config

You need to run your mongo nodes with -auth (or authenticat = true set in config) and if you run a replicaset, those nodes need to share a key file or kerberos authentication. (see http://docs.mongodb.org/manual/reference/user-privileges/) Let's assume, that all works for now. Now you need to specify the users. One way of doing that is the following:

- add the user for mongo to your main database (in our case tst)
- add an admin user for your own usage from shell to admin db (with all privileges)
- add the clusterAdmin user to admin db as well, grant read access to local

```js
    use admin
    db.addUser({user:"adm",pwd:"adm",
                       roles:["read","clusterAdmin"],
                       otherDBRoles:{local:["read"]}
                      })
    db.addUser({user:"admin",pwd:"admin",
                      roles:["dbAdminAnyDatabase",
                                "readWriteAnyDatabase",
                                "clusterAdmin",
                                "userAdminAnyDatabase"]
                       })

    use morphium_test
    db.addUser({user:"tst",pwd:"tst",roles:["readWrite","dbAdmin"]})
```

Here morphium_test is your application database morphium is connected to primarily. The admin db is a system database.

This is far away from being a complete guide, I hope this just gets you started with authentication....

# Common problem: timestamps on your data

This is something quite common: you want to know, when your data was last changed and maybe who did it. Usually you keep a timestamp with your object and you need to make sure, that these timestamps are updated accordingly. Morphium does this automatically - just declare the annotations:

```java
 @Entity
    @NoCache
    @LastAccess
    @LastChange
    @CreationTime
    public static class TstObjLA {
        @Id
        private ObjectId id;

        @LastAccess
        private long lastAccess;

        @LastChange
        private long lastChange;

        @CreationTime
        private long creationTime;

        private String value;

        public long getLastAccess() {
            return lastAccess;
        }

        public void setLastAccess(long lastAccess) {
            this.lastAccess = lastAccess;
        }

        public long getLastChange() {
            return lastChange;
        }

        public void setLastChange(long lastChange) {
            this.lastChange = lastChange;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public void setCreationTime(long creationTime) {
            this.creationTime = creationTime;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
```

You might ask, why do we need to specify, that access time is to be stored for the class and the field. The reason is: Performance! In order to search for a certain annotation we need to read all fields of the whole hierarchy the of the corresponding object which is rather expensive. In this case, we only search for those access fields, if necessary. All those are stored as long - System.currentTimeMillies()

## Explanation:

- @StoreLastAccess: Stores the last time, this object was read from db! Careful with that one...
- @StoreCreationTime: Stores the creation timestamp
- @StoreLastChange: Timestamp the last moment, this object was stored.

# Synchronizing Caches between nodes

It's a common problem, especially in clustered environments. How to synchronize caches on the different nodes. Morphium offers a simple solutions for it: On every write operation, a Message is stored in the Message queue (see MessagingSystem) and all nodes will clear the cache for the corresponding type (which will result in re-read of objects from mongo - keep that in mind if you plan to have a hundred hosts on your network) This is easy to use, does not cause a lot of overhead. Unfortunately it cannot be more efficient hence the Cache in Morphium is organized by searches.

the Morphium cache Syncrhonizer does not issue messages for uncached entities or entities, where clearOnWriteis set to false. Configurations are always synced - if you need a host-local configuration, you need to name it uniquely (by adding the hostname or mac address or something). BUT: All configuration will be synchronized to all nodes...

Here is an example on how to use this:

```java
    MorphiumMessaging m = morphium.createMessaging();
    m.setPause(10000).setMultithreadded(true);
    MessagingCacheSynchronizer cs = new MessagingCacheSynchronizer(m, morphium);
```

Actually this is all there is to do, as the CacheSynchronizer registers itself to both morphium and the messaging system.

# Change sinces 1.4.0

Now the Caching is specified by every entity in the @Cache annotation using one Enum called SyncCacheStrategy. Possible Values are: NONE (Default), CLEAR_TYPE_CACHE (clear cache of all queries on change) and UPDATE_ENTRY (updates the entry itself), REMOVE_ENTRY_FROM_TYPE_CACHE (removes all entries from cache, containing this element)

```java
enum SyncCacheStrategy {NONE, CLEAR_TYPE_CACHE, REMOVE_ENTRY_FROM_TYPE_CACHE, UPDATE_ENTRY}
```

UPDATE_ENTRY only works when updating records, not on drop or remove or update (like inc, set, push...). For example, if UPDATE_ENTRY is set, and you drop the collection, type cache will be cleared.
**Attention:** UPDATE_ENTRY will result in dirty reads, as the Item itself is updated, but not the corresponding searches!
Meaning: assume you have a Query result cached, where you have all Users listed which have a certain role:

```java
   Query<User> q=morphium.createQueryFor(User.class);
   q=q.f("role").eq("Admin");
   List<User> lst=q.asList();
```

Let's further assume you got 3 Users as a result. Now imagine, one node on your cluster changes the role of one of the users to something different than "Admin". If you have a list of users that might be changed while you use them! Careful with that! More importantly: your cache holds a copy of that list of users for a certain amount of time. During that time you will get a dirty read. Meaning: you will get objects that actually might not be part of your query or you will not get that actually might (not so bad actually).

Better use REMOVE_ENTRY_FROM_TYPE_CACHE in that case, as it will keep everything in cache except your search results containing the updated element. Might also cause a dirty read (as the newly added elements might not be added to your results) but it keeps findings more or less correct.

As all these synchronizations are done by sending messages via the Morphium own messaging system (which means storing messages in DB), you should really consider just disabling cache in case of heavy updates as a read from Mongo might actually be lots faster then sync of caches.

Keep that in Mind!

# Change since 1.3.07

Since 1.3.07 you need to add a autoSync=true to your cache annotation, in order to have things synced. It turned out, that automatic syncing is not always the best solution. So, you can still manually sync your caches. **This was removed in Morphium V3.x**

# Change since 4.0.0

We added a lot of new features in Morphium V4.0.0. And we built a couple of great thing based on the new `ChangeStream` feature of mongodb. This is bringing kind of push-notification to morphium. We use that for example for messaging and it brings a lot of advantages there

But also for Cache Synchronization this is great. Witch V4.0.0 we added a `WatchingCacheSynchronizer` and some listeners for it. This will automatically clear or update your caches (depending on your cache annontation settings), whenever a change to the collection will happen. This is way faster than using messaging, more direct. But Only works if you are in a replicaset environment!

# Manually Syncing the Caches

The sync in Morphium can be controlled totally manually (since 1.3.07), just send your own Clear-Cache Message using the corresponding method in CacheSynchronizer.

```java
   cs.sendClearMessage(CachedObject.class,"Manual delete");
```

# Caching in Morphium

Caching is very important even for such a extremely fast database as mongo. There are two kinds of caches: read cache and write cache.
#Write cache
The WriteCache is just a buffer, where all things to write will be stored and eventually stored to database. This is done by adding the Annotation @WriteBuffer to the class:

```java
@Entity
 @WriteBuffer(size = 150, strategy = WriteBuffer.STRATEGY.DEL_OLD)
    public static class BufferedBySizeDelOldObject extends UncachedObject {

    }
```

In this case, the buffer has a maximum of 150 entries, and if the buffer has reached that maximum, the oldest entries will just be deleted from buffer and hence NOT be written!
Possible strategies are:

- WriteBuffer.STRATEGY.DEL_OLD: delete oldest entries from buffer - use with caution
- WriteBuffer.STRATEGY.IGNORE_NEW: Do not write the new entry - just discard it. use with caution
- WriteBuffer.STRATEGY.JUST_WARN: just log a warning message, but store data anyway
- WriteBuffer.STRATEGY.WRITE_NEW: write the new entry synchronously and wait for it to be finished
- WriteBuffer.STRATEGY.WRITE_OLD: write some old data NOW, wait for it to be finished, than queue new entries

That's it - rest is 100% transparent - just call `morphium.store(entity);` - the rest is done automatically

# Morphium and Jcache Implementation

The morphium cache implementation follows the Jcache API and can be used in other places as well, if you wish. But the better way to make use of it is to have morphium use an existing JCAche-Provider in your application. For Example if you use EHCache in your system, you might want morphium to use that as well.
This is done by changing the cache implementation in `MorphiumConfig`:

```
MorphiumConfig config=new MorphiumConfig();
...
setCache(new MorphiumCacheJCacheImpl());
```

With this code, Morphium will use the Jcache implementation that is available in your system. If you have several cache managers, you might consider setting the "correct" one in MorphiumCacheJCacheImpl directly.

# Read Cache

Read caches are defined on type level with the annotation @Cache. There you can specify, how your cache should operate:

```java
@Cache(clearOnWrite = true, maxEntries = 20000, strategy = Cache.ClearStrategy.LRU, syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE, timeout = 5000)
@Entity
public class MyCachedEntity {
.....
}
```

here a cache is defined, which has a maximum of 20000 entries. Those Entries have a lifetime of 5 seconds (`timeout=5000`). Which means, no element will stay longer than 5sec in cache. The strategy defines, what should happen, when you read additional object, and the cache is full:

- Cache.ClearStartegy.LRU: remove least recently used elements from cache
- Cache.ClearStrategy.FIFO: first in first out - depending time added to cache
- Cahce.ClearStrategy.RANDOM: just remove some random entries

With `clearOnWrite=true` set, the local cache will be erased any time you write an entity of this typte to database. This prevents dirty reads. If set to false, you might end up with stale data (for as long as the timeout value) but produce less stress on mongo and be probably a bit faster.

CacheSynchronization is something important in clustered or replicated environments. In this case, the Write event is propagated to all cluster members. See [Cache Syncrhonization](https://github.com/sboesebeck/morphium/wiki/Cache-Synchronization) for more details.

**Internals / Implementation details **

- Morphium uses the cache based on the search query, sort options and collection overrides given. This means that there might be doublicate cache entries. In order to minimize the memory usage, Morphium also uses an ID-Cache. So all results are just added to this id cache and those ids are added as result to the query cache.
  the Caches are organized per type. This means, if your entity is not marked with @Cache, queries to this type won't be cached, even if you override the collection name.
- The cache is implemented completely unblocking and completely thread safe. There is almost no synchronized block in morphium.

# Complex Data structures

In the jUnit tests, morphium is tested to support those complex data structure, like lists of lists, lists of maps or maps of lists of entities. I think, you'll get the picture:

```java
  public static class CMapListObject extends MapListObject {
        private Map<String, List<EmbObj>> map1;
        private Map<String, EmbObj> map2;
        private Map<String, List<String>> map3;
        private Map<String, List<EmbObj>> map4;

        private Map<String, Map<String, String>> map5;
        private Map<String, Map<String, EmbObj>> map5a;
        private Map<String, List<Map<String, EmbObj>>> map6a;

        private List<Map<String, String>> map7;
        private List<List<Map<String, String>>> map7a;
        ....
```

All those are tested in JUnit and can be stored and read accordingly...

# Simple queries

most of your queries probably are simple ones. like searching for a special id or value. This is done rather simply with the query-Object: morphium.createQueryFor(MyEntity.class).f("field").eq(value) if you add more f(fields) to the query, they will be concatenated by a logical AND. so you can do something like:

```java
    Query<UncachedObject> q=morphium.createQueryFor(UncachedObject.class);
    q.f("counter").gt(10).f("counter").lt(20);
This would result in a query like: "All Uncached Objects, where counter is greater than 10 and counter is less then 20".
```

# Or Queries

in addition to those AND-queries you can add an unlimited list of queries to it, which will be concatenated by a logical OR.

```java
   q.f("counter").lt(100).or(q.q().f("value").eq("Value 12"), q.q().f("value").eq("other"));
This would create a query like: "all UncachedObjects where counter is less than 100 and (value is 'value 12' or value is 'other')"
```

the Method q() creates a new empty query for the same object. It's a convenience Method. Please be careful, never use your query Object in the parameter list of or - this would cause and endless loop! ATTENTION here!

This gives you the possibility to create rather complex queries, which should handle about 75% of all cases. Although you can also add some NOR-Queries as well. These are like "not or"-Queries....

```java
   q.f("counter").lt(100).nor(q.q().f("counter").eq(90), q.q().f("counter").eq(55));
this would result in a query like: "All query objects where counter is less than 100 and not (counter=90 or counter=55).
```

this adds another complexity level to the queries ;-)

If that's not enough, specify your own query
You can also specify your own query object (BasicDBObject from MongoDB) in case of a very complex query. This is part of the Query-Object and can be used rather easily:

```java
        BasicDBObject query=new BasicDBObject();
        query=query.append("counter",new BasicDBObject("$lt",10));
        Query<UncachedObject> q=MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        List<UncachedObject> lst=q.complexQuery(query);
```

Although, in this case the query is a very simple one (counter < 10), but I think you get the Idea....

# Limitations

Well, the fluent query interface does have its limitations. So its not possible to have a certain number of or-concatenated queries (like (counter==14 or Counter <10) and (counter >50 or counter ==30)). I'm not sure, this is very legible... maybe we should replace f by field and a like... what do you think?

# Customization

Morphium can be customized in many ways. The configuration contains several settings that might be useful for your own custom implementation.

- AggregatorClass and AggregatorFactory - used for the aggregation framework. Take a closer look
  here [Aggregator Framework support](https://github.com/sboesebeck/morphium/wiki/Aggregator-Framework-Support)
- ObjectMapper: If you want to implement your own mapping algorithm you'd set this to your implementation. Default
  is `de.caluga.morphium.objectmapping.ObjectMapperImpl`
- Cache : you can define your own cache, if you want to.
- Field: MongoFields are used during querying: query.f() returns a mongoField. Here all operators are implemented. If you want an additional operator to the already existing ones, you could to this here
- IteratorClass: Used for Morphiums paged iterator. Can be customized as well
- All Writer implementations (AsyncWriter, BufferedWriter, Writer): If you want to tailor the behavior of the writer to your needs, you can set your implementation to be used by morphium. Be careful: both bufferedWriter and AsyncWriter use the Writer
- and of course: the query object itself can be replaced with your own implementation.

This makes Morphium very flexible and not only because of this feature used by a lot of huge projects.

# Index management in morphium

Morphium is capable of managing all indices, you might need for your database. In Mongo, indices are very, very important and the lack of which might increase the execution time of simple queries a log (like times 100).
You can specify your indexes at class and field level. But you always must mark your entity with Indexes.
Indexes will be created by morphium if you write your first data to a non existent collection. This is done to prevent morphium from creating new indexes on huge collections.
You can enforce indexing for a given type by calling `ensureIndexesFor`

```java
morphium.ensureIndexesFor(MyEntity.class);
```

Indexes can be specified on class level, and for a property - see those examples:

```java
@Entity
@Index //just a marker
public class MyEntity {
   @Id String myId;
   @Index
   String name;

   @Index(options={"unique:true"})
   String uid;
}
```

In this example, there are three Indices defined, one on the ID, one on name and one on uid. All of them are defined in natural order. If you want to define an index, which consists of several fields, you need to do that at class level:

```java
@Entity
@Index({"-name,uid","ts"}) //defining indices
public class MyEntity {
   @Id String myId;
   String name;
   long ts;

   @Index(options={"unique:true"})
   String uid;
}
```

In this case, one unique index for uid is defined, one compound index for name and uid (where name is sorted in reverse order), one single index for the field ts and one index for the ID of this entity. As you see, it's possible to define most of the indices in one @Index-Annotation at class level.
Its also possible to add additional options, like 2d-Indexes or text indexes:

```java
 @Index("position:2d")  //defining a gespacial index
```

# Ensure Index in Morphium

The easiest way to make sure that indices are in place is by calling the method morphium.ensureIndex(). This one creates one index on the given Collection (or Entity respectively). Usage is as follows:

```java
   morphium.ensureIndex(UncachedObject.class,"-timestamp","name"); //will create on combined index
   morphium.ensureIndex(CachedObject.class,CachedObject.Fields.counter); //will create on +1 index on this type
   Map<String, Integer> complexIndex=new HashMap<String,Integer>(); //Make it a sorted map, if necessary!
   complexIndex.put("counter",-1);
   complexIndex.put("name",1);
   complexIndex.put("_id",-1);
   morphium.ensureIndex(MyEntity.class,complexIndex);
```

Be careful: Index creation might cause your Mongo to freeze for a while depending on the size of your collections.

# Automatic Index creation

Morphium supports automatic index creation. But those indexes are only created, when the collection is about to be newly created (usually, this is what you want, especially in conjunction with the new NameProvider support). Just add the annotations to your type or field to have it indexed:

```java
  @Cache(clearOnWrite=false,writeCache=true,readCache=false,overridable=false)
  @Entity
  @Index({"timestamp","timestamp,level","host_name,timestamp","hostname,level,-timestamp","position:2d"})
  public class MyLog  {
     private long timestamp;
     @Index(decrement=false)
     private String hostName;
     private String level;
     public List<Double> position;
     @Id
     private String id;
     @Index(options={"unique:true"})
     private String uniqueField;
     //additional fields and methods
     ....
  }
```

This will create several indexes, if the collection is about to be created (and only then):

- timestamp
- timestamp and level
- host_name and timestamp
- hostname, level and timestamp (-1)
- host_name
- a 2D-Index for geospacial search on the field position
- and a unique index for the unique_field

When you have added the Index-Annotation to your entities, you can also use the Method morphium.ensureIndex(MyEntity.class) to honor those calls. This will ensure indices now, no matter whether the collection exists or not (use with care!)

# Morphium's Lazy Loading of references

morphium supports lazy loading of references. This is easy to use, just add `@Reference(lazyLoading=true)` to the reference you want to have lazy loaded.

```java
@Entity
public class MyEntity {
   ....
   @Reference(lazyLoading=true)
   private UncachedObject myReference;  //will be loaded when first accessed
   @Reference
   private MyEntity ent; //will be loaded when this object is loaded - use with caution
                         //this could cause an endless loop
   private MyEntity embedded; //this object is not available on its own
                              //its embedded as subobject in this one
}
```

# how does it work

When a reference is being lazy loaded, the corresponding field will be set with a Proxy for an instance of the correct type, where only the ObjectID is set. Any access to it will be caught by the proxy, and any method will cause the object to be read from DB and unmarshalled. Hence this object will only be loaded upon first access.

It should be noted that when using Object.toString(); for testing that the object will be loaded from the database and appear to not be lazy loaded. In order to test Lazy Loading you should load the base object with the lazy reference and access it directly and it will be null. Additionally the referenced object will be null until the references objects fields are accessed.

# Logging

Morphium used Log4J til V2.2.20, which caused some trouble in a multithreadded, heavy load environment.

Right now, there is a very lightweight implementation of a logger, that is being used for morphiums internal logging. You can use this Logger class, if you want, as it is very lightweight and easy to use. It is being configured either using MorphiumConfig, or using JVM Parameters:

- `-Dmorphium.log.level=5` would set maximum log output, e.g. DEBUG. Levels are: 0 - no logging, 1 - FATAL only, 2 - also errors, 3 - include Warnings, 4 - add info to the list, 5 - debug messages also
- `-Dmorphium.log.synced=false` if set to true, the output is unbuffered, causing every message to be written instantanously (usually used with STDOUT as output)
- `-Dmorphium.log.file=FILENAME` - filename might also be `-` or STDOUT (both meaning stadard output) or STDERR for standard error

You also can specify an suffix for those options, which represent the logger's name (usually a class or packagename). For example, `-Dmorphium.log.level.de.caluga.morphium.Morphium=0` would switch off all messages from this class, `-Dmorphium.log.level.de.caluga.morphium.aggregation=5` would switch on debugging for all messages coming from classes in that package.

Of course, you can configure the logger using MorphiumConfig, there are the following methods:

```
   setGlobalLogLevel(int level);
   setGloablLogFile(String fileName);
   setGlobalLogSynced(boolean sync);

   setLogLevelForPrefix(String prfx);
   setLogLevelForClass(Class cls);

   setLogFileForPrefix(String prfx);
   setLogFileForClass(Class cls);

   setLogSyncedForPrefix(String prfx);
   setLogSyncedForClass(Class cls);
```

All those settings might also be set using environment variables, dots replaced by underscores (in Unix like: export morphium_log_level=5)

_Attention_: This logger class is by design not really threadsafe, you should create an own instance for every thread. The Logger won't create any errors in multithreaded environments, but might create some strange output
_Attention_: Also, keep in mind that every instance of `Logger` will hold an open link to outputfile.

The logger class is more or less compatible with Log4J (at least most of the methods are).

The configuration of the logger is only read when initialized! So if you change your config, it won't effect your logger instances.

##LoggerDelegate##

Introduced with V2.2.23BETA3 you can define a LogDelegate to be used instead of a File, just add the Prefix `class:` to the filename, e.g. `-Dmorphium.log.file=class:de.caluga.morphium.Log4JLoggerDelegate` would use a simple Log4J implementation.

Of course you can define your own LoggerDelegate, just implement the interface `de.caluga.morphium.LoggerDelegate` and add your classname as file.

_Attention_: The log delegate will be instantiated with every new Logger instance!

# Mapping Objects from and to Strings

## Why strings?

yes, the whole Mongo communication is based on BSON-Objects which is totally fine for performance. BSON Objects can very easily be cast to a "normal" Json string using the .toString() method. But unfortunately i did not find a way back... Sometimes it might be useful to be able to parse strings stored in files or even in Mongo -> endless possibilities ;-)

## How does it work

I added one new dependency to a "Simple JSon Parser" which does exactly, what the name states: simply parsing Json. In our case into BSon representation. This can than easily be put into the "normal" mapping algorithm to create the corresponding Java-Class.

```java
    String json=morphium.getMapper().marshall(myEntity).toString();
    MyEntity myEntity=morphium.getMapper().unmarshall(MyEntity.class,json)
```

## Caveats

When de-serializing your json string, you need to know, which class to use to de-serialize the data into. If you have several different objects stored in your json text, you probably should set @Embedded(polymorph=true) as annotation to your entity. (Polymorph objects cause morphium to store the classname) One other thing: if you don't want to use ids in your object, use the @Embedded annotation...

ATTENTION: right now, this string parsing can parse one Json object at a time, so you cannot have several objects stored in one string. If there is need for changing that, contact me...

# Morphium paged iterator

## Description

Problem is, when dealing with huge tables or lots of data, you'd probably include paging to your queries. You would read data in chunks of for example 100 objects to avoid memory overflows. This is now available by Morphium. The new MorphiumIterator works as Iterable or Iterator - whatever you like. It's included in the Query-interface and can be used very easily:

```java
Query<Type> q=morphium.createQueryFor(Type.class);
q=q.f("field").eq..... //whatever

for (Type t:q.asIterable()) {
   //do something with t
}
```

This creates an iterator, reading all objects from the query in chunks of 10... if you want to read them one by one, you only need to give the chunk-size to the call:

```java
for (Type t:q.asIterable(1)) {
   //now reads every single Object from db
}
```

# The Iterator

You can also use the iterator as in the "good ol' days".

```java
   Iterator<Type> it=q.asIterable(100);  //reads objects in chunks of 100
   while (it.hasNext()) {
    ... //do something
   }
```

If you use the MorphiumIterator as the type it actually is, you'd get even more information:

```java
   MorphiumIterator<Type> it=q.asIterable(100);
   it.next();
   ....
   long count=it.getCount(); //returns the number of objects to be read
   int cursorPos=it.getCursor(); //where are we right now, how many did we read
   it.ahead(5); //jump ahead 5 objects
   it.back(4); //jump back
   int bufferSize=it.getCurrentBufferSize(); //how many objects are currently stored in RAM
   List<Type> lst=it.getCurrentBuffer(); //get the objects in RAM
```

**Attention**: the count is the number of objects matching the query at the instanciation of the iterator. This ensures, that the iterator terminates. The Query will be executed every time the buffer boundaries are reached. It might cause unexpected results, if the sort of the query is wrong.

For example:

```java
   //created Uncached Objects with counter 1-100; value is always "v"
   Query<UncachedObject> qu=morphium.createQueryFor(UncachedObject.class).sort("-counter");
   for (UncachedObject u:qu.asIterable()) {
       UncachedObject uc=new UncachedObject();
            uc.setCounter(u.getCounter()+1);
            uc.setValue("WRONG!");
            MorphiumSingleton.get().store(uc);
            log.info("Current Counter: "+u.getCounter()+" and Value: "+u.getValue());
   }
```

The output is as follows:

```
14:21:10,494 INFO  [main] IteratorTest: Current Counter: 100 and Value: v
14:21:10,529 INFO  [main] IteratorTest: Current Counter: 99 and Value: v
14:21:10,565 INFO  [main] IteratorTest: Current Counter: 98 and Value: v
14:21:10,610 INFO  [main] IteratorTest: Current Counter: 97 and Value: v
14:21:10,645 INFO  [main] IteratorTest: Current Counter: 96 and Value: v
14:21:10,680 INFO  [main] IteratorTest: Current Counter: 95 and Value: v
14:21:10,715 INFO  [main] IteratorTest: Current Counter: 94 and Value: v
14:21:10,751 INFO  [main] IteratorTest: Current Counter: 93 and Value: v
14:21:10,786 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:10,822 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:10,857 INFO  [main] IteratorTest: Current Counter: 96 and Value: WRONG!
14:21:10,892 INFO  [main] IteratorTest: Current Counter: 95 and Value: v
14:21:10,927 INFO  [main] IteratorTest: Current Counter: 95 and Value: WRONG!
14:21:10,963 INFO  [main] IteratorTest: Current Counter: 94 and Value: v
14:21:10,999 INFO  [main] IteratorTest: Current Counter: 94 and Value: WRONG!
14:21:11,035 INFO  [main] IteratorTest: Current Counter: 93 and Value: v
14:21:11,070 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,105 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:11,140 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:11,175 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:11,210 INFO  [main] IteratorTest: Current Counter: 94 and Value: WRONG!
14:21:11,245 INFO  [main] IteratorTest: Current Counter: 94 and Value: WRONG!
14:21:11,284 INFO  [main] IteratorTest: Current Counter: 93 and Value: v
14:21:11,328 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,361 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,397 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,432 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:11,467 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:11,502 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:11,538 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:11,572 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,607 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,642 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,677 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,713 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,748 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:11,783 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:11,819 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:11,853 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:11,889 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:11,923 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,958 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:11,993 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:12,028 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:12,063 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:12,098 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,133 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,168 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,203 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,239 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:12,273 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:12,308 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:12,344 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:12,379 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:12,413 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,448 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,487 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,521 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,557 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,592 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:12,626 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:12,662 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:12,697 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:12,733 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,769 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,804 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,839 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,874 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,910 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:12,945 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:12,980 INFO  [main] IteratorTest: Current Counter: 93 and Value: WRONG!
14:21:13,015 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:13,051 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,085 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,121 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,156 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,192 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,226 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,262 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,297 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:13,331 INFO  [main] IteratorTest: Current Counter: 92 and Value: v
14:21:13,367 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,403 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,446 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,485 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,520 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,556 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,592 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,627 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,662 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:13,697 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,733 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,768 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,805 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,841 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,875 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,911 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,946 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:13,982 INFO  [main] IteratorTest: Current Counter: 92 and Value: WRONG!
14:21:14,017 INFO  [main] IteratorTest: Current Counter: 91 and Value: v
14:21:14,017 INFO  [main] IteratorTest: Cleaning up...
14:21:14,088 INFO  [main] IteratorTest: done...
```

The first chunk is ok, but all that follow are not. Fortunately count did not change or in this case, the iterator would never stop. Hence, if your collection changes while you're iterating over it, you might get inexpected results. Writing to the same collection within the loop of the iterator is generally a bad idea...

## Advanced Features

Since V2.2.5 the morphium iterator supports lookahead (prefetching). This means its not only possible to define a window size to step through your data, but also how many of those windows should be prefetched, while you step through the first one.

This works totally transparent for the user, its just a simple call to activate this feature:

```
theQuery.asIterable(1000,5); //window size 1000, 5 windows prefetch
```

Since 2.2.5 the morphium iterator is also able to be used by multiple threads simultaneously. This means, several threads access the _same_ iterator. This might be useful for querying and alike.
To use that, you only need to set setMultithreaddedAccess to true in the iterator itself:

```
MorphiumIterator<MyEntity> it=theQuery.asIterable(1000,15)
it.setMultithreaddedAccess(true);
```

_Attention_: Setting mutlithreaddedAccess to true will cause the iterator to be a bit slower as it has to do some things in a `synchronized` fashion.

# Partially Updatable Entities

The idea behind partial updates is, that only the changes to an entity are transmitted to the database and will thus reduce the load on network and Mongodb itself. Partially updatable entities is the implementation in Morphium and can be used in several ways.

## Application driven

This is the easiest way - you already know, what fields you changed and maybe you even do not want to store fields, that you actually did change. In that case, call the updateUsingFields-Method:

```java
   UncachedObject o....
   o.setValue("A value");
   o.setCounter(105);
   Morphium.get().updateUsingFields(o,"value"); //does only send updates for Value to mongodb
                                                                         //counter is ignored
```

updateUsingFields honors the lifecycle methods as well as caches (write cache or clear read_cache on write). take a look at some code from the corresponding junit-test for better understanding:

```java
 UncachedObject o... //read from MongoDB
            o.setValue("Updated!");
            Morphium.get().updateUsingFields(o, "value");
            log.info("uncached object altered... look for it");
            Query<UncachedObject> c=Morphium.get().createQueryFor(UncachedObject.class);
            UncachedObject fnd= (UncachedObject) c.f("_id").eq( o.getMongoId()).get();
            assert(fnd.getValue().equals("Updated!")):"Value not changed? "+fnd.getValue();
```

## Application driven - 2nd way

Implement the interface PartiallyUpdateablewhich ensures, that one method is implemented in your entity, returning all fields that should be persisted with the next save operation.

```java
@Entity
public class SimpleEntitiy  implements ParitallyUpdateable {
      private String v1;
      @Property(fieldName="name")
      private String theName;

     ....
      private List<String> updateFields=new ArrayList<String>();
      ....

      public void setV1(String v) {
            v1=v;
            updateFields.add("v1");
     }

     public void setTheName(String n) {
          theName=n;
          updateFields.add("name");
     }
    List<String> getAlteredFields() {
         return updateFields;
    }
```

This should illustrate, how this works. When store with such an entity-object is called, morphium checks whether or not PartiallyUpdateable is implemented and calls getAlteredFields to do a partial update (by calling updateUsingFields)

## Through proxy

Morphium is able to create a transparent proxy for your entities, taking care of all this mentioned above. See here the code from the JUnit-Test:

```java
        UncachedObject uo=new UncachedObject();
        uo=Morphium.get().createPartiallyUpdateableEntity(uo);
        assert(uo instanceof PartiallyUpdateable):"Created proxy incorrect";
        uo.setValue("A TEST");
        List<String> alteredFields = ((PartiallyUpdateable) uo).getAlteredFields();
        for (String f:alteredFields) {
            log.info("Field altered: "+f);
        }
        assert(alteredFields.contains("value")):"Field not set?";
```

The Object created by createPartiallyUpdateableEntity is a proxy object which does intercept all set-Method calls.
BUT: to be sure, that the correct field is stored in your List of altered fields, the method should have a UpdatingField annotation attached, to specify the field, that this setter does alter. This is actually only necessary, if the field name differs from the setter-name (according to standard java best practices). Usually, if you have a setter called setTheValue, the field theValue is altered. Unless you changed the field name with a @Property-Annotation, you do not need to do anything here... By rule of thumb: If you have a @Property-Annotation to your fields, you should also add the corresponding @UpdatingField-Annotation to the setter...

Actually, this sounds more complicated as it is ;-)

## using the @PartialUpdate Annotation

The current version supports automate parial updates. You only have to add the @PartialUpdate Annotation to your class and off you go. Attention: there are some things you need to take care of:

when you use @PartialUpdate Annotation to your class definition, all instances created by reading this object from Mongo will be embedded in a proxy, which does take car of the partial updates for you (see above createPartiallyUpdateableEntity). This may cause problems with Reflection or .getClass()-Code
When you create the object yourself, it's not partial updateable - you need to call createPartiallyUpdateableEntity manually

This mechanism relays on coding convention CamelCase, which means, setter need to be named after the property they set - for Example setName should set the property name and setLastName the property lastName (as usual)
if you have a non-setter Method (like incSomething) which does modify a Property, you need to add the @PartialUpdate-Annotation to the method specifying the property, which will be changed after calling this, e.g.

```java
@Entity
@PartialUpdate
class PUTest {
    private int counter;
    //getter do not change values - hence not interesting for PartialUpdates
    public int getCounter() {
       return counter;
    }
    //Setter Name follows coding convention -> will work fine
    public void setCounter(int c) {
        counter=c;
    }

    //as this modifies a field, but is not named according to the setter-rule, you need to specify
    //the corresponding field
   @PartialUpdate("counter")
    public void inc() {
        counter++;
    }
}
```

you can add the annotation to any method you like, the specified field will be add to the modified filed list accordingly
it's not possible yet to specify more than one field to be changed or add the annotation more than once
this code is not (yet) tested in conjunction with lazy loading - it might cause problems here dereferencing entities

## Limitations

This code is rather new and not heavily tested yet, the code is implemented honoring maps, lists and embedded entities, but there is not Unit-Test for those cases jet.
It does not work for embedded entities yet (and I'm not sure how to implement that feature - if you have ideas, just contribute).

# Polymorphism in Morphium

Morphium supports polymorphism. That means, you can store objects of different kind in one collection and process them with a single call. Please keep these things in mind when using polymorphsim in morphium:

- you need to make sure that all types are stored in the same collection. Usually you would do that by providing an own name provider
- all entities in the collection should have a common superclass. Otherwise there would be strange "ClassCastExceptions"
- when accessing the collection, you should always use the supertype. otherwise data might be lost. And, if storing an update, the type might change.

lets get in more detail about the last item. Imaginge you stored Several objects in one collection, all of those of type Person. Now you add an Employee, which is as subclass of person, to this collection.
When you access the collection as Employee, all your persons will read into Employee objects - which means, this might break constraints or might even fail.

here is a testcase for morphium, which shows how polymorphism might be used:

```java
package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 27.11.13
 * Time: 08:39
 * Test polymorphism mechanism in Morphium
 */
public class PolymorphismTest extends MongoTest {
    @Test
    public void polymorphTest() throws Exception {
        MorphiumSingleton.get().dropCollection(PolyTest.class);
        OtherSubClass p = new OtherSubClass();
        p.setPoly("poly");
        p.setOther("other");
        MorphiumSingleton.get().store(p);

        SubClass sb = new SubClass();
        sb.setPoly("poly super");
        sb.setSub("sub");
        MorphiumSingleton.get().store(sb);

        assert (MorphiumSingleton.get().createQueryFor(PolyTest.class).countAll() == 2);
        List<PolyTest> lst = MorphiumSingleton.get().createQueryFor(PolyTest.class).asList();
        for (PolyTest tst : lst) {
            log.info("Class " + tst.getClass().toString());
        }
    }

    public static class PolyNameProvider implements NameProvider {

        @Override
        public String getCollectionName(Class<?> type, ObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {
            return "poly";
        }
    }

    @Entity(polymorph = true, nameProvider = PolyNameProvider.class)
    @NoCache
    public static abstract class PolyTest {
        private String poly;

        @Id
        private ObjectId id;

        public String getPoly() {
            return poly;
        }

        public void setPoly(String poly) {
            this.poly = poly;
        }
    }

    public static class SubClass extends PolyTest {
        private String sub;

        public String getSub() {
            return sub;
        }

        public void setSub(String sub) {
            this.sub = sub;
        }
    }

    public static class OtherSubClass extends PolyTest {
        private String other;

        public String getOther() {
            return other;
        }

        public void setOther(String o) {
            this.other = o;
        }
    }
}
```

#Name provider

mongo is really fast and stores a lot of date in no time. Sometimes it's hard then, to get this data out of mongo again, especially for logs this might be an issue (in our case, we had more than a 100 million entries in one collection). It might be a good idea to change the collection name upon some rule (by date, timestamp whatever you like). Morphium supports this using a strategy-pattern.

```java
public class DatedCollectionNameProvider implements NameProvider{
    @Override
    public String getCollectionName(Class<?> type, ObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {
        SimpleDateFormat df=new SimpleDateFormat("yyyyMM");
        String date=df.format(new Date());
        String ret=null;
        if (specifiedName!=null) {
            ret=specifiedName+="_"+date;
        } else {
                String name = type.getSimpleName();
                if (useFQN) {
                    name=type.getName();
                }
            if (translateCamelCase) {
                name=om.convertCamelCase(name);
            }
            ret=name+"_"+date;
        }


        return ret;
    }
}

```

This would create a monthly named collection like "my_entity_201206". In order to use that name provider, just add it to your @Entity-Annotation:

```java
@Entity(nameProvider = DatedCollectionNameProvider.class)
public class MyEntity {
....
}
```

## Performance

The name provider instances themselves are cached for each type upon first use, so you actually might do as much work as possible in the constructor.
BUT: on every read or store of an object the corresponding name provider method `getCollectionName` is called, this might cause Performance drawbacks, if you logic in there is quite time consuming.

# Retries on network error

## Write Concern is not enough

The write concern aka WriteSafety-Annotation in morphium is not enough for being on the safe side. the WriteSafety only makes sure, that, if all is ok, data is written to the amount of nodes, you want it to be written. You define the safety level more or less in an Application point of view. This does not affect networking outage or other problems. Hence, you can set several retry-Settings...

## retry settings in Writers

Morphium has 3 different types of writers:

- the normal writer: supports asynchronous and snychronous writes
- the async writer: forces asyncnhrounous writes
- the buffered writer: stores write requests in a buffer and executes those on block

This has some implications, as the core of morphium is asynchrounous, we need to make sure, there are not too many pending writes. (the "pile" is determined by the maximum amount of connections to mongo - hence this is something you won't need to configure)
This is where the retry settings for writers come in. When writing data, this data is either written synchronously or asynchonously. In the latter case, the requests tend to pile up on heavy load. And we need to handle the case, when this pile gets too high. This is the retry. When the pile of pending requests is too high, wait for a speicified amount of time and try again to queue the operation. If that fails for all retries - throw an exception.

## Retry settings for Network errors

As we have a really sh... network which causes problems more than once a day, I needed to come up with a solution for this as well. As our network does not fail for more than a couple of requests, the idea is to detect network problems and retry the operation after a certain amount of time. This setting is specified globally in morphium config:

```java
        morphium.getConfig().setRetriesOnNetworkError(10);
        morphium.getConfig().setSleepBetweenNetworkErrorRetries(500);
```

This causes morphium to retry any operation on mongo 10 times (if a network related error occurs) and pause 500ms between each try. This includes, reads, writes, updates, index creation and aggregation. If the access failed after the (in this case) 10th try - rethrow the networking error to the caller.

# InfluxDB Driver for Morphium 3.x

This is an implementation of the `MorphiumDriver` interface in the [morphium](https://github.com/sboesebeck/morphium) project.

Morphium was built to be a mongodb abstraction layer and driver. With Version `3.0` a new Driver architecture was introduced, making morphium able to work with theoretically any kind of database.

InfluxDB is a time series db which is something very different to mongodb and does not have a complex ql like a full blown SQL-DB.

Of course, using this driver will have some drawbacks, you will not be able to do everything, you could do with the CLI or a native driver.

## Usecases

- send timeseries data to influxdb in an efficient and "known" way utelizing all the good stuff, morphium does offer (bulk requests etc)
- simple cluster support
- do simple queries to influx, gathering high level information

what this driver will **not** do:

- help you visualizing things
- administration of influxdb

## How To

quite simple. Instanciate Morphium as usual, just set a different driver:

```java
        MorphiumConfig cfg = new MorphiumConfig("graphite", 100, 1000, 10000);
        cfg.setDriverClass(InfluxDbDriver.class.getName());
        cfg.addHostToSeed("localhost:8086");
        cfg.setDatabase("graphite");

        cfg.setLogLevelForClass(InfluxDbDriver.class, 5);
        cfg.setGlobalLogSynced(true);
        Morphium m = new Morphium(cfg);
```

Sending Data do influx works same as if it was a mongodb:

```java
            EntTest e = new EntTest();
            //fill with data ...
            m.store(e);
```

reading from influx works also similar to mongodb, with minor changes:

```java
  Query<EntTest> q=m.createQueryFor(EntTest.class).f("host").eq(hosts[0]).f("lfd").gt(12);
        q.addProjection("reqtime","mean");
        q.addProjection("ret_code","group by");
        List<EntTest> lst=q.asList();
        System.out.println("Got results:"+lst.size());
```

you need to set the aggregation method as projection operator. Also the `group by` clause is modeled ther.

And one major _hack_: if you want to do queries using time constraints like `where time > now() - 10d` you need to query using `time()`as morphium needs to detect that this is not a field but a function you're calling:

```java
 q=m.createQueryFor(EntTest.class).f("host").eq(hosts[1]).f("lfd").gt(12).f("time()").gt("now() - 100d");
        q.addProjection("reqtime","mean");
        q.addProjection("ret_code","group by");
```

# Text Search

Mongodb has since V3.x a built in text search functionality. This can be used in commandline, or using morphium:

```java
  @Test
    public void textIndexTest() throws Exception {
        morphium.dropCollection(Person.class);
        try {
            morphium.ensureIndicesFor(Person.class);

        } catch (Exception e) {
            log.info("Text search not enabled - test skipped");
            return;
        }
        createData();
        TestUtils.waitForWrites(morphium,log);
        Query<Person> p = morphium.createQueryFor(Person.class);
        List<Person> lst = p.text(Query.TextSearchLanguages.english, "hugo", "bruce").asList();
        assert (lst.size() == 2) : "size is " + lst.size();
        p = morphium.createQueryFor(Person.class);
        lst = p.text(Query.TextSearchLanguages.english, false, false, "Hugo", "Bruce").asList();
        assert (lst.size() == 2) : "size is " + lst.size();
    }

```

In this case, there is some Data begin created, which puts the name of some superheroes in a mongo. Searching for the text ist something different than searching via regular expressions, because Text Indexes are way more efficient in that case.

If you need more information on text indexes, have a look at Mongodbs documentation and take a look at the Tests for TextIndexes within the sourcecode of morphium.
