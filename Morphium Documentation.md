# Morphium Documentation

{{TOC}}

## What is _Morphium_

_Morphium_ started as a feature rich access layer and POJO mapper for MongoDB in java. It was built with speed and flexibility in mind. So it supported cluster aware caching out of the box, lazy loading references and much more. The POJO Mapping is the _core_ of _Morphium_, all other features were built around that. It makes accessing MongoDB easy, supports all great features of MongoDB and adds some more.

But with time, the MongoDB based messaging became one of the most popular features in _Morphium_. It is fast, reliable, customisable and stable.

### _Morphium V5_ 
With morphium V5 we did a _big_ rewrite and started at the bottom: the dirver to
access MongoDB. The official MongoDB-Java-Driver does have a lot more features,
that morphium either did implement differently or just does not use. Hence a way
smaller, easier to maintain Driver helps a lot. 

We started writing a mongodb wire protocol dirver, that supports Mongodb 5 and
upwards. It is tested with mongo 5 and 6 (Morphium V5.0.5). It is minimalistic
and built especially for _Morphium_'s needs.

We then started adapting the Morphium Driver Interface, integrating it into
_Morphium_ itself. Hence, V4 and V5 are _mostly_ source compatible with each
other, but some driver related calls and settings are just working differently
now. So when upgrading: please be aware

**Caveat: Morphium V5 is working with JDK11 and following - no JDK1.8 support
anymore!**

## About this document

This document is a documentation for _Morphium_ in the current (5.0) version. It would be best, if you had a basic
understanding of MongoDB and have it installed and maybe used _Morphium_ already. If you want to know about MongoDB's features,
that _Morphium_ makes available in java and are referenced here, have a look at the official MongoDB pages and the
documentation there.

Later in this document there are chapters about the POJO mapping, querying data and using the aggregation framework.
Also a chapter about the InMemory driver, which is quite useful for testing. But let's start with the messaging
subsystem first.

## Using _Morphium_ as a message queueing system

_Morphium_ itself is simple to use, easy to customise to your needs and was built for high performance and scalability. The messaging system is no different. It relies on the `watch` functionality, that MongoDB offers since V3.6 (you can also use messaging with older versions of MongoDB, but it will result in polling for new messages). With that feature, the messages are _pushed_ to all listeners. This makes it a very efficient messaging system based on MongoDB.

### why _Morphium_ message queueing

There is a ton of messaging solutions out there. All of them have their advantages and offer lots of features. But only few of them offer the things that _Morphium_ has. And to be exact, _Morphium Messaging_ is no real messaging system and is not intended to be a replacement for a proper RabbitMQ or similiar installation. But _Morphium Messagin_ offers some specific features, that might come in handy for some solutions:

- the message queue can easily be inspected and you can use any mongo-client (like `mongosh`) and do search queries to find the messages you are looking for[^you can even use aggregation on it, to gather more information about your messages]
- the message queue can be altered (update single messages with ease, delete messages or just _add_ new messages) with any mongo-client.
- Possibility to broadcast messages, that are only processed by one client max (Exclusive Messages) - similar to a `topic` in other messaging systems.
- With V4.2 of _Morphium_ this also works with a group of recipients.
- Messaging is multithreaded and thread safe
- pausing and unpausing of message processing without data loss (meaning, you
  will get messages that have been sent, even while you did not process
  messages. For example, the client pauses messageProcessing, while it runs some
  elaborate task. This takes several seconds. During that time, 2 additional
  messages come in. As soon as the client unpauses the message processing, it
  will _also_ process those messages according to priority and timestamp.
- _Morphium_ messaging picks up all pending messages on startup - no data loss.
- no need to install additional servers or provide separate infrastructure. Just use your MongoDB you likely already have in place.

There are people out there using _Morphium_ and its messaging for production grade development. For example [Genios.de](https://www.genios.de) uses Morphium messaging to power a microservice architecture with an enterprise message bus.

### Quick start Messaging

```java
Morphium m=new Morphium();
Messaging messaging=new Messaging(m);

messaging.addMessageListener((messaging, msg) -> {
            log.info("Got message!");
            return null;  //not sending an answer
        });
```

This is a simple example of how to implement a message consumer. This consumer listens to _all_ incoming messages, regardless of name.

Messages do have some fields, that you might want to use for your purpose. But you can create your own message type as well (see below). The Msg-Class defines those properties:

- `name` the name of the Message - you can define listeners only listening to messages of a specific name using `addListenerForMessageNamed`. Similar to a _topic_ in other messaging systems
- `msg`: String message
- `value`: well - a String value
- `mapValue`: for more complex use cases where you need to send more information
- `additional`: list value - used for more complex use cases
- all messages do store some values for the processing algorithm, like `processed_by`, `in_answer_to`, `timestamp`, `locked`, `locked_by` etc. you should _not_ use those fields for your own purpose!

So if you want to send a Message, that is also simple:

```java
messaging.queueMessage(new Msg("name","A message","the value");
```

queueMessage is running asynchronously, which means, that the message is _not_ directly stored. If you need more speed and shorter reaction time, you should use `sendMessage` instead (directly storing message to mongo).

### Answering messages

_Morphium_ is able to answer any message for you. Your listener implementation only needs to return an instance of
the `Msg`-Class. This will then be sent back to the sender as an answer.

When sending a message, you also may wait for the incoming answer. The Messaging class offers a method for that purpose:

    //new messaging instance with polling frequency of 100ms, not multithreaded
    //polling only used in case of non-Replicaset connections and in some
    //cases like unpausing to find pending messages

        Messaging sender = new Messaging(_Morphium_, 100, false);
        sender.start();

        gotMessage1 = false;
        gotMessage2 = false;
        gotMessage3 = false;
        gotMessage4 = false;

        Messaging m1 = new Messaging(_Morphium_, 100, false);
        m1.addMessageListener((msg, m) -> {
            gotMessage1 = true;
            return new Msg(m.getName(), "got message", "value", 5000);
        });

        m1.start();
        Thread.sleep(2500);

        Msg answer = sender.sendAndAwaitFirstAnswer(new Msg("test", "Sender", "sent", 15000), 15000);
        assertNotNull(answer);;
        assert (answer.getName().equals("test"));
        assertNotNull(answer.getInAnswerTo());;
        assertNotNull(answer.getRecipient());;
        assert (answer.getMsg().equals("got message"));
        m1.terminate();
        sender.terminate();

As the whole communication is asynchronous, you will have to specify a timeout after wich the wait for answer will be aborted with an exception. And, there might be more than one answers to the same message, hence you will only get the first one.

in the above example, the timeout for the answer is set to 15s (and the TTL for messages also).

### more advanced settings

#### Custom message classes

As mentioned above, you can define your own Message-Class to be send back and forth. This class just needs to extend the standard `Msg`-Class. When adding a listener to messaging, you have the option to also use generics to specify the Msg-Type you want to use.

#### Message priorities

Every message does have a priority field. That is used for giving queued messages precedence over others. The priority could be changed _after_ a message is queued directly in MongoDB (or using _Morphium_).

But as the messaging is built on pushing of messages, when is the priority field used? Several cases:

- when starting up messaging. When starting Messaging, the system does look for pending messages in the queue, highes prio is used first
- when unpausing a messaging instance, it will look for any messages in the queue and will process them according to their priority.

#### Pausing / unpausing of messaging

In some cases it might be necessary to pause message processing for a time. That might be the case, if the message is triggering some long running task or so. If so, it would be good not to process any additional messages (at least of that type).

You can call `messaging.pauseProcessingOfMessagesNamed` to _not_ process any more messages of a certain type.

_Attention_: if you have long running tasks triggered by messages, you should pause processing in the onMessage method and unpause it when finished.

#### Multithreading / Multimessage processing

When instantiating Messaging, you can specify two booleans:

- multithreading: if true, every incoming message will be processed in an own thread (Executor - see MorphiumConfig
  below). That means, several messages can be processed in parallel
- processMultiple: this setting is only important in case of startup or unpausing. If true, messaging will lock all
  messages available for this listener and process them one by one (or in parallel if multithreading is enabled). These
  settings are influenced by other settings:
- `messagingWindowSize` in MorphiumConfig or as constructor parameter / setter in Messaging: this defines how many
  messages are marked for processing at once. Those might be processed in parallel (depending whether `processMultiple`
  is true, and the executor configuration, how many threads can be run in parallel)
- `useChangeStream` in Messaging. Usually messaging determines by the cluster status, whether or not to use the changestream or not. If in a cluster, use it, if not use polling. But if you explicitly want to use polling, you can set this value to `false`. The advantage here might be, that the messages are processed by priority with every poll. This might be useful depending on your usecase. If this is set to false (or you are connected to an single instance), the `pause` configuration option (aka polling frequency) in Messaging will determine how fast your messages can be consumed. **Attention** high polling frequency (a low `pause` value), will increase the load on MongoDB.
- `ThreadPoolMessagingCoreSize` in MorphiumConfig: If you define messaging to be multithreaded it will spawn a new thread with each incoming message. this is the core size of the corresponding thread pool. If your messaging instance is not configured for multithreading, this setting is not used.
- `ThreadPoolMessagingMaxSize`: max size of the thread pool. similar to above.
- `ThreadPoolMessagingKeepAliveTime`: time of threads to live in ms
  some examples to clarify that:
- your messaging instance is configured for multithreaded processing, multiple processing, having a `windowSize` of 100 and a `ThreadPoolMessagingMaxSize` of 10, then there will be 100 messages in queue marked for being processed by this specific messaging instance, but only 10 will be processed in parallel.
- multithreaded processing is false, then the `windowSize` determines how many messages are marked for being processed, but are only processed one by one
- multithreaded processing and multiple processing is false, then only one message is marked for being processed at a time. As soon as this processing is finished, the next message is being taken.
- having `multithreaded` set to true and `processMultiple` set to false would result in running each message processing in one separate thread, but only one at a time. This is very similar to having `multithreaded` and `process multiple` both set to false.

#### Custom MessageQueue name

When creating a Messaging instance, you can set a collection name to use. This could be compared to having a separate message queue in the system. Messages sent to one queue are not being registered by another.

#### JMS Support

_Morphium_ messaging also implements the standard JMS-API to a certain extend and can be used this way. Please keep in mind that JMS does not support most of the features, _Morphium_ messaging offers, and that the JMS implementation does not cover 100% of the JMS API yet:

```java
@Test
    public void basicSendReceiveTest() throws Exception {
        JMSConnectionFactory factory = new JMSConnectionFactory(morphium);
        JMSContext ctx1 = factory.createContext();
        JMSContext ctx2 = factory.createContext();

        JMSProducer pr1 = ctx1.createProducer();
        Topic dest = new JMSTopic("test1");

        JMSConsumer con = ctx2.createConsumer(dest);
        con.setMessageListener(message -> log.info("Got Message!"));
        Thread.sleep(1000);
        pr1.send(dest, "A test");

        ctx1.close();
        ctx2.close();
    }

     @Test
    public void synchronousSendRecieveTest() throws Exception {
        JMSConnectionFactory factory = new JMSConnectionFactory(morphium);
        JMSContext ctx1 = factory.createContext();
        JMSContext ctx2 = factory.createContext();

        JMSProducer pr1 = ctx1.createProducer();
        Topic dest = new JMSTopic("test1");
        JMSConsumer con = ctx2.createConsumer(dest);

        final Map<String, Object> exchange = new ConcurrentHashMap<>();
        Thread senderThread = new Thread(() -> {
            JMSTextMessage message = new JMSTextMessage();
            try {
                message.setText("Test");
            } catch (JMSException e) {
                e.printStackTrace();
            }
            pr1.send(dest, message);
            log.info("Sent out message");
            exchange.put("sent", true);
        });
        Thread receiverThread = new Thread(() -> {
            log.info("Receiving...");
            Message msg = con.receive();
            log.info("Got incoming message");
            exchange.put("received", true);
        });
        receiverThread.start();
        senderThread.start();
        Thread.sleep(5000);
        assertNotNull(exchange.get("sent"));;
        assertNotNull(exchange.get("received"));;
    }
```

**Caveats:**

The JMS Implementation uses the answering mechanism for acknowledging incoming messages. This makes JMS more or less half as fast as the direct usage of _Morphium_ messaging.

Also, the implementation is very basic at the moment. A lot of methods lack implementation[^those throw an Exception to let you know, it is missing]. If you notice some missing functionality, just open an issue at [github](https://github.com/sboesebeck/morphium).

Because of the JMS Implementation being very basic at the moment, it should not be considered production ready!

### Examples

#### Simple producer consumer setup:

```java
Morphium m=new Morphium(config);
// create messaging instance with default settings, meaning
// no multithreading, windowSize of 100, processMultiple false
Messaging producer=new Messaging(m);

producer.queueMessage(new Msg("name","a message","a value"));

the receiver needs to connect to the same mongo and the same database:

Morphium m=new Morphium(config);
Messaging consumer=new Messaging(m);
consumer.start(); //needed for receiving messages

consumer.addMessageListener((messaging, msg) -> {
         //Incoming message
         System.out.println("Got a message of name "+msg.getName());
         return null; //no answer to send back
        });
```

you can also register listeners only for specific messages:consumer.start(); //needed for receiving messages

```java
consumer.addListenerForMessageNamed("name",(messaging, msg) -> {
       //Incoming message, is always named "name"
         System.out.println("Got value: "+msg.getValue());
         Msg answer=new Msg(msg.getName(),"answer","the answerValue");
         return answer; //no answer to send back
        });
```

**Attention**: the producer will only be able to process incoming messages, if `start()` was called!

The message sent there was a broadcast message. All registered listeners will receive that message and will process it!

#### Direct messages

In order to send a message directly to a specific messaging instance, you need to get the unique ID of it. This id is add as sender to any message.

    Msg m=new Msg("Name","Message","value");
    m.addRecipient(messaging1.getId());
    //you could add more recipients if necessary

_Background_: This is used to send answers back to the sender. If you return a message instance in `onMessage`, this message will be sent directly back to the sender.

You can add as many recipients as needed, if no recipient is defined, the message by default is sent to all listeners.

#### Exclusive Broadcast messages

Broadcast messages are fine for informing all listeners about something. But for some more complex scenarios, you would need a way to queue a message, and have only one listener process it - no matter which one (load balancing?)

_Morphium_ supports this kind of messages, it is called "exclusive broadcast". This way, you can easily scale up by just adding listener instances.

Sending a exclusive broadcast message is simple:

    	Msg m=new Message("exclusive","The message","and value");
    	m.setExclusive(true);
    	messaging.queueMessage(m);

The listener only need to implement the standard `onMessage`-Method to get this message. Due to some sophisticated locking of messages, _Morphium_ makes this message exclusive - which means, it is only processed once!

Since _Morphium_ V4.2 it is also possible to send an exclusive message to certain recipients[^does only make sense, when there is more than one recipient usually].

The behaviour is the same: the message will only be processed by _one_ of the specified recipients, whereas it will be processed by _all_ recipients, if not exclusive.

### changing behaviour

#### mark message as already processed

By default, messages are marked as processed _after_ the listener's `onMessage`
is finishes. If you need this behaviour to be changed, implement the method
`markAsProcessedBeforeExec` and have it return `true`, depending on your needs

#### auto release locks

An exclusivee message is being locked by a listener before it might be
processed. This lock by default exists limitless, meaning if one listner locked
a message for itself, the lock is never releaser
IF you wan to modify this behaviour, you can set a non 0 value to
`autoUnlockAfter`in messaging instance. This will make sure, that locks are
removed after the specified amount of milliseconds and a message might be
processed by others. Caveat: do not set this value to low as it might interfere
with message processing. Good values should be significantly larger than the
pause setting on messaging.

#### timeout specifications

Usually, messages just time out, they are being deleted by mongodb when the
timeout is reached (default 30 seconds). But it might be useful to have the
message around longer, not timing out until it is processed.

There are two settings in a Msg-Object that specify that

- `boolean deleteAfterProcessing` : if true, message will be marked for deletion
  after processing
  - `int deleteAfterProcessingTime`: time offset (in ms) when this message
    should be deleted.

these delete flags, do work in combination. E.g. by default messages are deleted
after 30seconds, but directly after processing (`deleteAfterProcessingTime=0`
and `deleteAfterProcessing=true`).

## InMemory Driver

One main purpose of the `InMemoryDriver` is to be able to do testing without having a MongoDB installed. The InMemoryDriver adds the opportunity to let all MongoDB-code run in Memory, with a couple of exceptions

- the inMemoryDriver is also not capable to return cluster information, run mongodb commands
- it does not support spacial indexes or queries
- it is limited with javascript functionality (like $where queries)
- the InMemoryDriver prior to V4.2.0 did not have the ability to do
  aggregations. 
- With Morphium V5.0 the InMemoryDriver also gained Expr-support in aggregations
  and queries 

### how to use the inMemory Driver

you just need to set the Driver properly in your _Morphium_ configuration.

    	MorphiumConfig cfg = new MorphiumConfig();
    	cfg.addHostToSeed("inMem");
    	cfg.setDatabase("test");
    	cfg.setDriverName(InMemoryDriver.driverName);
    	cfg.setReplicasetMonitoring(false);
    	morphium = new Morphium(cfg);

Of course, the _InMemDriver_ does not need hosts to connect to, but for compatibility reasons, you need to add at least one host (although it will be ignored).

You can also set the Driver in the settings, e.g. in properties:

    morphium.driverName = "InMemDriver"

After that initialisation you can use this _Morphium_ instance as always, except that it will "persist" data only in Memory.

### Dumping InMemory data

As in memory storage is by definition not lasting, it might be a good idea to store your data onto disk for later use. The InMemoryDriver does support that:

```java
 @Test
public void driverDumpTest() throws Exception {
    for (int i = 0; i < 100; i++) {
        UncachedObject e = new UncachedObject();
        e.setCounter(i);
        e.setValue("value" + i);
        e.setIntData(new int[]{i, i + 1, i + 2});
        e.setDval(42.00001);
        e.setBinaryData(new byte[]{1, 2, 3, 4, 5});
        morphium.store(e);

        ComplexObject o = new ComplexObject();
        o.setEinText("A text " + i);
        o.setEmbed(new EmbeddedObject("emb", "v1", System.currentTimeMillis()));
        o.setRef(e);
        morphium.store(o);


    }

    ByteArrayOutputStream bout = new ByteArrayOutputStream();

    InMemoryDriver driver = (InMemoryDriver) morphium.getDriver();
    driver.dump(morphium, morphium.getDriver().listDatabases().get(0), bout);
    log.info("database dump is " + bout.size());

    driver.close();
    driver.connect();
    driver.restore(new ByteArrayInputStream(bout.toByteArray()));
    assert (morphium.createQueryFor(UncachedObject.class).countAll() == 100);
    assert (morphium.createQueryFor(ComplexObject.class).countAll() == 100);

    for (ComplexObject co : morphium.createQueryFor(ComplexObject.class).asList()) {
        assertNotNull(co.getEinText());;
        assertNotNull(co.getRef());;
    }
}
```

In this example, data is stored to a binary stream, which could also be stored to disk somewhere.

But you can also create a dump in _JSON_ format, which makes it easier to edit and maybe to create from scratch:

```java

@Test
public void jsonDumpTest() throws Exception {

    MorphiumTypeMapper<ObjectId> mapper = new MorphiumTypeMapper<ObjectId>() {
        @Override
        public Object marshall(ObjectId o) {
            Map<String, String> m = new HashMap<>();
            m.put("value", o.toHexString());
            m.put("class_name", o.getClass().getName());
            return m;

        }

        @Override
        public ObjectId unmarshall(Object d) {
            return new ObjectId(((Map) d).get("value").toString());
        }
    };
    morphium.getMapper().registerCustomMapperFor(ObjectId.class, mapper);
    for (int i = 0; i < 10; i++) {
        UncachedObject e = new UncachedObject();
        e.setCounter(i);
        e.setValue("value" + i);
        morphium.store(e);
    }
    ExportContainer cnt = new ExportContainer();
    cnt.created = System.currentTimeMillis();

    cnt.data = ((InMemoryDriver) morphium.getDriver()).getDatabase(morphium.getDriver().listDatabases().get(0));

    Map<String, Object> s = morphium.getMapper().serialize(cnt);
    System.out.println(Utils.toJsonString(s));

    morphium.dropCollection(UncachedObject.class);
    ExportContainer ex = morphium.getMapper().deserialize(ExportContainer.class, Utils.toJsonString(s));
    assertNotNull(ex);;
    ((InMemoryDriver) morphium.getDriver()).setDatabase(morphium.getDriver().listDatabases().get(0), ex.data);

    List<UncachedObject> result = morphium.createQueryFor(UncachedObject.class).asList();
    assert (result.size() == 10);
    assert (result.get(1).getCounter() == 1);
}


@Entity
public static class ExportContainer {
    @Id
    public Long created;
    public Map<String, List<Map<String, Object>>> data;
}
```

The JSON output of this little dump looks like this:

```json
{
  "_id": 1599853076411,
  "data": {
    "uncached_object_0": [
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b51"
        },
        "counter": 0,
        "dval": 0,
        "value": "value0"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b53"
        },
        "counter": 1,
        "dval": 0,
        "value": "value1"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b55"
        },
        "counter": 2,
        "dval": 0,
        "value": "value2"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b57"
        },
        "counter": 3,
        "dval": 0,
        "value": "value3"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b59"
        },
        "counter": 4,
        "dval": 0,
        "value": "value4"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b5b"
        },
        "counter": 5,
        "dval": 0,
        "value": "value5"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b5d"
        },
        "counter": 6,
        "dval": 0,
        "value": "value6"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b5f"
        },
        "counter": 7,
        "dval": 0,
        "value": "value7"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b61"
        },
        "counter": 8,
        "dval": 0,
        "value": "value8"
      },
      {
        "_id": {
          "class_name": "org.bson.types.ObjectId",
          "value": "5f5bd214f8fd82e792ef3b63"
        },
        "counter": 9,
        "dval": 0,
        "value": "value9"
      }
    ]
  }
}
```

## _Morphium_ POJO Mapping

### Ideas and design criteria

In the early days of MongoDB there were not many POJO mapping libraries available. One was called _morphia_. Unfortunately we had a lot of problems adapting this to our needs.

Hence we built **Morphium** and we named it similar to _morphia_ to show where the initial idea came from.

_Morphium_ is built with flexibility, thread safety, performance and cluster awareness in mind.

- thread safety: all aspects of _Morphium_ were tested multithreaded so that it can be used in production
- performance: one of the main goals of _Morphium_ was to improve performance. The Object Mapping in use is a custom
  implementation that was built especially for _Morphium_, is very fast (faster than other Json-Mappers) and to improve speed even further, caching is
  part of the core features of _Morphium_
- cluster awareness: this is essential nowadays for high availability or just mere speed. _Morphium_s caches are all
  cluster aware (if configured to be) which means you will not end up with dirty reads in a clustered environment when using \_Morphium_
- independent from mongoDB Driver: _Morphium_ does not have a direct dependency on the mongoDB java driver, instead it
  considers it to be provided. This means, you can have a different version of the driver in use than the one _Morphium_
  was last tested with (you do not need the latest and grates, usually it is backward compatible). In addition to
  that, _Morphium_ does not directly use MongoDB or BSON classes but offers its own implementation. For example
  the `MorphiumId`, wich is a drop in replacement for `ObjectId`. With V5.0 this
  independence was put to the next level - Morphium uses it's own driver to
  access MongoDB (see above)
- Clear Design Idea: code for reading from MongoDB is encapsulated in `Query` or `QueryImpl` respectively. All code for
  writing to MongoDB is encapsulated in `Morphium` itself. For convenience there are some calls from one to another, but
  the actual code is located as stated.

### Concepts

_*Morphium*_ is built to be very flexible and can be used in almost any environment. So the architecture needs to be
flexible and sustainable at the same time. Hence it's possible to use your own implementation for the cache if you want
to.

There are four major components of _*Morphium*_:

1. the _*Morphium*_ Instance: This is you main entry point for interaction with Mongo. Here you create Queries and you
   write data to mongo. All writes will then be forwarded to the configured Writer implementation, all reads are handled
   by the Query-Object
2. Query-Object: you need a query object to do reads from mongo. This is usually created by
   using `_Morphium_.createQueryFor(Class<T> cls)`. With a Query, you can easily get data from database or have some
   things changed (update) and alike.
3. the Cache: For every request that should be sent to mongo, _*Morphium*_ checks first, whether this collection is to be cached and if there is already a result being stored for the corresponding request.
4. The Writers: there are 3 different types of writers in _*Morphium*_: The Default Writer (`_Morphium_Writer`) - writes directly to database, waiting for the response, the BufferedWriter (`BufferedWriter`) - does not write directly. All writes are stored in a buffer which is then processed as a bulk. The last type of writer ist the asynchronous writer (`AsyncWriter`) which is similar to the buffered one, but starts writing immediately - only asynchronous. _*Morphium*_ decides which writer to use depending on the configuration and the annotations of the given Entities. But you can _always_ use asynchronous calls just by adding a`AsyncCallback` implementation to your request.

Simple rule when using _*Morphium*_: You want to read -> Use the Query-Object. You want to write: Use the _*Morphium*_ Object.

There are some additional features built upon this architecture:

- messaging: _*Morphium*_ has its own production grade messaging system. Its has a lot of features, that are unique for a messaging system.
- cache synchronization: Synchronize caches in a clustered environment. Uses messaging.
- custom mappers - you can tell _*Morphium*_ how to map a certain type from and to MongoDB. For example there is a "custom" mapper implementation for mapping `BigInteger` instances to MongoDB.
- every one of those implementations can be changed: it is possible to set the class name for the `BufferedWriter` to a custom built one (in `MorphiumConfig`). Also you could replace the object mapper with your own implementation by implementing the `ObjectMapper` interface and telling _Morphium_ which class to use instead. In short, these things can be changed in _Morphium_ / MorphiumConfig:
  - MorphiumCache
  - ObjectMapper
  - Query
  - Field
  - QueryFactory
  - Aggregator
  - AggregatorFactory
  - MorphiumDriver (> V3.0, for connecting to MongoDB or any other data source if you want to. For example, there is an In-Memory-Driver you might want to use for testing. As an example, there is also an InfluxDB-Driver available.)
- Object Mapping from and to Strings (using the object mapper) and JSON.
- full support for the Aggregation Framework
- Transaction support (for supporting MongoDB versions)
- Automatic encryption of fields (this is a re-implementation of the MongoDB enterprise feature in pure java - works declarative)

### Advantages / Features

#### POJO Mapping

_Morphium_ is capable of mapping standard Java objects (POJOs - plain old java objects) to MongoDB documents and back. This should make it possible to seemlessly integrate MongoDB into your application.

#### Declarative caching

When working with databases - not only NoSQL ones - you need to consider caching. _Morphium_ integrates transparent
declarative caching by entity to your application, if needed. Just define your caching needs in the `@Cache` annotation.
The cache uses any JavaCache compatible cache implementation (like EHCache), but provides an own implementation if
nothing is specified otherwise.

There are two kinds of caches: read cache and write cache.

**Write cache**:

The WriteCache is just a buffer, where all things to write will be stored and eventually stored to database. This is done by adding the Annotation `@WriteBuffer` to the class:

```java
@Entity
 @WriteBuffer(size = 150, strategy = WriteBuffer.STRATEGY.DEL_OLD)
    public static class BufferedBySizeDelOldObject extends UncachedObject {

    }
```

In this case, the buffer has a maximum of 150 entries, and if the buffer has reached that maximum, the oldest entries will just be deleted from buffer and hence NOT be written!
Possible strategies are:

- `WriteBuffer.STRATEGY.DEL_OLD`: delete oldest entries from buffer - use with caution
- `WriteBuffer.STRATEGY.IGNORE_NEW`: Do not write the new entry - just discard it. use with caution
- `WriteBuffer.STRATEGY.JUST_WARN`: just log a warning message, but store data anyway
- `WriteBuffer.STRATEGY.WRITE_NEW`: write the new entry synchronously and wait for it to be finished
- `WriteBuffer.STRATEGY.WRITE_OLD`: write some old data NOW, wait for it to be finished, than queue new entries

That's it - rest is 100% transparent - just call `morphium.store(entity);` - the rest is done automatically.

internally it uses the `BufferedWriter` implementation, which can be changed, if needed (see configuration options below). Also, some config settings exist for switching off the buffered writing altogether - comes in handy when testing. have a closer look at the configuration options in `MorphiumConfig` which refer to `writeBuffer` or `BufferedWriter`.

**Read Cache**

Read caches are defined on type level with the annotation @Cache. There you can specify, how your cache should operate:

```java
@Cache(clearOnWrite = true, maxEntries = 20000, strategy = Cache.ClearStrategy.LRU, syncCache = Cache.SyncCacheStrategy.CLEAR_TYPE_CACHE, timeout = 5000)
@Entity
public class MyCachedEntity {
.....
}
```

here a cache is defined, which has a maximum of 20000 entries. Those Entries have a lifetime of 5 seconds (timeout=5000). Which means, no element will stay longer than 5sec in cache. The strategy defines, what should happen, when you read additional object, and the cache is full:

- `Cache.ClearStartegy.LRU`: remove least recently used elements from cache
- `Cache.ClearStrategy.FIFO`:first in first out - depending time added to cache
- `Cache.ClearStrategy.RANDOM`: just remove some random entries
  With `clearOnWrite=true` set, the local cache will be erased any time you write an entity of this typte to database. This prevents dirty reads. If set to false, you might end up with stale data (for as long as the timeout value) but produce less stress on mongo and be probably a bit faster.

#### cache synchronization

as mentioned above, caching is of utter importance in production grade applications. Usually, caching in a clustered Environment is kind of a pain. As you need consider dirty reads and such. But _Morphium_ caching works also fine in a clustered environment. Just start (instantiate) a `CacheSynchronizer` - and you're good to go!

There are two implementations of the cache synchronizer:

- `WatchingCacheSynchronizer`: uses mongodbs `watch` - Feature to get informed about changes in collections via push.
- `MessagingCacheSynchronizer`: uses messaging to inform cluster members about changes. This one has the advantage that you can send messages manually or when other events occur

**Internals / Implementation details **

- _Morphium_ uses the cache based on the search query, sort options and collection overrides given. This means that there might be duplicate cache entries. In order to minimize the memory usage, _Morphium_ also uses an ID-Cache. So all results are just added to this id cache and those ids are added as result to the query cache.
  the Caches are organized per type. This means, if your entity is not marked with @Cache, queries to this type won't be cached, even if you override the collection name.
- The cache is implemented completely unblocking and completely thread safe. There is almost no synchronized block in _Morphium_.

It's a common problem, especially in clustered environments. How to synchronize caches on the different nodes. _Morphium_ offers a simple solutions for it: On every write operation, a Message is stored in the Message queue (see MessagingSystem) and all nodes will clear the cache for the corresponding type (which will result in re-read of objects from mongo - keep that in mind if you plan to have a hundred hosts on your network) This is easy to use, does not cause a lot of overhead. Unfortunately it cannot be more efficient hence the Cache in _Morphium_ is organized by searches.

the _Morphium_ cache synchronizer does not issue messages for uncached entities or entities, where clearOnWrite is set to false.

Here is an example on how to use this:

```java
    Messaging m=new Messaging(morphium,10000,true);
    MessagingCacheSynchronizer cs=new MessagingCacheSynchronizer(m,morphium);
```

Actually this is all there is to do, as the CacheSynchronizer registers itself to both _Morphium_ and the messaging system.

**Change since 1.4.0**
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

As all these synchronizations are done by sending messages via the _Morphium_ own messaging system (which means storing messages in DB), you should really consider just disabling cache in case of heavy updates as a read from Mongo might actually be lots faster then sync of caches.

Keep that in mind!

**Change since 1.3.07**
Since 1.3.07 you need to add a autoSync=true to your cache annotation, in order to have things synced. It tuned out, that automatic syncing is not always the best solution. So, you can still manually sync your caches.

**Manually Syncing the Caches**
The sync in _Morphium_ can be controlled totally manually (since 1.3.07), just send your own Clear-Cache Message using the corresponding method in CacheSynchronizer.

```java
   cs.sendClearMessage(CachedObject.class,"Manual delete");
```

#### Auto-Versioning

When it comes to dirty reads and such, you might want to use the auto-versioning feature of _Morphium_. This will give every entity a version number. If you want to write to MongoDB and the version number differs, you'd get an exception - meaning the database was modified before you tried to persist your data. This so called _optimistic locking_ will help in most cases to avoid accidental overwriting of data.
To use auto-Versioning, just set the corresponding flag in the `@Entity`-annotation to `true` and define a `Long` in your class, that should hold the version number using the `@Version`-annotation.

**Attention:** do not change the version value manually, this will cause problems writing and will most probably cause loss of data!

#### Type IDs

usually _Morphium_ knows which collection holds which kind of data. When de-serializing it is easy to know, what class to instanciate.
But when it comes to polymorphism and containers (like lists and maps), things get compicated. _Morphium_ adds in this case the class name as property to the document. Up until version 4.0.0 this was causing some problems when refactoring your Entities. If you changed the classname or the package name of that class, de-serializing was impossible (the classname was obviously wrong).

now you can just set the `typeId` in `@Entity` to be able refactor more easily. If you already have data, and you want to refactor your entitiy names, just add the _original_ class name as type id!

#### Sequences

One of the very convenient features of SQL-Databases is the support for sequences. This is _very_ useful when trying to have unique IDs.
_Morphium_ implements a feature very similar to SQL-Sequences. Hence it is also called `SequenceGenerator`.

A sequence is a simple implementation in _Morphium_ that uses MongoDB to generate unique numbers. Example:

```
SequenceGenerator sg = new SequenceGenerator(morphium, "tstseq", 1, 1);
long v = sg.getNextValue();
assert (v == 1) : "Value wrong: " + v;
v = sg.getNextValue();
assert (v == 2);
```

As those generators use MongoDB for synchronization, they are cluster-safe and can be used by all clients of the same MongoDB simultaneously. No number will be delivered twice!

This test here uses several Threads to access the same `SequenceGenerator`:

```java
 final SequenceGenerator sg1 = new SequenceGenerator(morphium, "tstseq", 1, 0);
        Vector<Thread> thr = new Vector<>();
        final Vector<Long> data = new Vector<>();
        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(() -> {
                for (int i1 = 0; i1 < 25; i1++) {
                    long nv = sg1.getNextValue();
                    assert (!data.contains(nv)) : "Value already stored? Value: " + nv;
                    data.add(nv);
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                    }
                }
            });
            t.start();
            thr.add(t);
        }
        log.info("Waiting for threads to finish");
        for (Thread t : thr) {
            t.join();
        }
        long last = -1;
        Collections.sort(data);
        for (Long l : data) {
            assert (last == l - 1);
            last = l;
        }
        log.info("done");
```

Here is an example, where the sequences are being used by _a lot_ of separate threads each with its own connection to mongodb:

```java
morphium.dropCollection(Sequence.class);
Thread.sleep(100); //wait for the drop to be persisted


//creating lots of sequences, with separate MongoDBConnections
//reading from the same sequence
//in different Threads
final Vector<Long> values=new Vector<>();
List<Thread> threads=new ArrayList<>();
final AtomicInteger errors=new AtomicInteger(0);
for (int i = 0; i < 10; i++) {
    Morphium m=new Morphium(MorphiumConfig.fromProperties(morphium.getConfig().asProperties()));

    Thread t=new Thread(()->{
        SequenceGenerator sg1 = new SequenceGenerator(m, "testsequence", 1, 0);
        for (int j=0;j<100;j++){
            long l=sg1.getNextValue();
            log.info("Got nextValue: "+l);
            if(values.contains(l)){
                log.error("Duplicate value "+l);
                errors.incrementAndGet();
            } else {
                values.add(l);
            }
            try {
                Thread.sleep((long) (100*Math.random()));
            } catch (InterruptedException e) {
            }
        }
        m.close();
    });
    threads.add(t);
    t.start();
}

while (threads.size()>0){
    //log.info("Threads active: "+threads.size());
    threads.get(0).join();
    threads.remove(0);
    Thread.sleep(100);
}

assert(errors.get()==0);

```

**Attention** after creating a new `SequenceGenerator` the `currentValue` will be `startValue-inc` in order so that `getNextValue()` will return `startValue` first.
When migrating to _Morphium_ 4.2.x or higher from older versions the sequences will not be compatible anymore due to a change in ID.
to fix that, you need to run the following command in mongoDB shell:

```js
db.sequence.find({ name: { $exists: true } }).forEach(function (x) {
  db.sequence.deleteOne({ _id: x._id });
  x._id = x.name;
  delete x.name;
  db.sequence.save(x);
});
```

#### transparent encryption of values

_Morphium_ implemented a client side version of auto encrypted fields. When defining a property, you can specify the value to be encrypted. _Morphium_ provides an implementation of AESEncryption, but you could implement any other encryption.
In order for encryption to work, we need to provide a `ValueEncryptionProvider`. This is a very simple interface:

```java
		package de.caluga.morphium.encryption;

		public interface ValueEncryptionProvider {
		    void setEncryptionKey(byte[] key);

		    void setEncryptionKeyBase64(String key);

		    void setDecryptionKey(byte[] key);

		    void sedDecryptionKeyBase64(String key);

		    byte[] encrypt(byte[] input);

		    byte[] decrypt(byte[] input);

		}
```

There are two implementations available: `AESEncryptionProvider` and `RSAEncryptionProvider`.
Another interface being used is the `EncryptionKeyProvider`, a simple system for managing encryption keys:

```java
		package de.caluga.morphium.encryption;

		public interface EncryptionKeyProvider {
		    void setEncryptionKey(String name, byte[] key);

		    void setDecryptionKey(String name, byte[] key);

		    byte[] getEncryptionKey(String name);

		    byte[] getDecryptionKey(String name);

		}
```

The `DefaultEncrptionKeyProvider` acutally is a very simple key-value-store and needs to be filled manually. The implementation `PropertyEncryptionKeyProvider` reads those keys from _encrypted_ property files.

Here is an example, on how to use the transparent encryption:

```java
    @Entity
    public static class EncryptedEntity {
        @Id
        public MorphiumId id;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public String enc;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public Integer intValue;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public Float floatValue;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public List<String> listOfStrings;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public Subdoc sub;


        public String text;
    }



    @Test
    public void objectMapperTest() throws Exception {
        morphium.getEncryptionKeyProvider().setEncryptionKey("key", "1234567890abcdef".getBytes());
        morphium.getEncryptionKeyProvider().setDecryptionKey("key", "1234567890abcdef".getBytes());
        MorphiumObjectMapper om = morphium.getMapper();
        EncryptedEntity ent = new EncryptedEntity();
        ent.enc = "Text to be encrypted";
        ent.text = "plain text";
        ent.intValue = 42;
        ent.floatValue = 42.3f;
        ent.listOfStrings = new ArrayList<>();
        ent.listOfStrings.add("Test1");
        ent.listOfStrings.add("Test2");
        ent.listOfStrings.add("Test3");

        ent.sub = new Subdoc();
        ent.sub.intVal = 42;
        ent.sub.strVal = "42";
        ent.sub.name = "name of the document";

		//serializing the document needs to encrypt the data
        Map<String, Object> serialized = om.serialize(ent);
        assert (!ent.enc.equals(serialized.get("enc")));

		//checking deserialization used decryption
        EncryptedEntity deserialized = om.deserialize(EncryptedEntity.class, serialized);
        assert (deserialized.enc.equals(ent.enc));
        assert (ent.intValue.equals(deserialized.intValue));
        assert (ent.floatValue.equals(deserialized.floatValue));
        assert (ent.listOfStrings.equals(deserialized.listOfStrings));
    }
```

Please note, that the key _name_ used for encryption and decryption is to be defined in the property configuration of the corresponding entity.

#### binary serialization

the config of morphium does have a setting called `objectSerializationEnabled`. When set to `true` this will cause morphium to use the standard binary serialization of the JDK to store _any_ instance of _any_ class that implements `serializable`[^attention: the "top level" document needs to be an Entity to have all necessary settings there. But "subdocuments"/properties might be just serializable].

Another setting in the config called `warnOnNoEntitySerialization` will create a warning message in log, when this serialization takes place.

This is set to `true` by default, to make development easier. But you probably do not want to use it on heavy load entities.

To store the binary data, _Morphium_ uses a helper class called `BinarySerializedObject`, which will be shown in MongoDB:

```json
{
    "_id" : ObjectId("5f5bc1d8f8fd8247688e41f5"),
    "list" : [
        {
            "original_class_name" : "de.caluga.test.mongo.suite.base.NonEntitySerialization$NonEntity",
            "_b64data" : "rO0ABXNyADtkZS5jYWx1Z2EudGVzdC5tb25nby5zdWl0ZS5Ob25FbnRpdHlTZXJpYWxpemF0aW9u\r\nJE5vbkVudGl0eV18gEK68jkAAgACTAAHaW50ZWdlcnQAE0xqYXZhL2xhbmcvSW50ZWdlcjtMAAV2\r\nYWx1ZXQAEkxqYXZhL2xhbmcvU3RyaW5nO3hwc3IAEWphdmEubGFuZy5JbnRlZ2VyEuKgpPeBhzgC\r\nAAFJAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAACp0ABZUaGFuayB5\r\nb3UgZm9yIHRoZSBmaXNo"
        },
        "Some string"
    ]
}
```

In this case, this "Container" does contain a list of non-entity objects:

```java

    @Entity
    public class NonEntityContainer {
        @Id
        private MorphiumId id;
        private List<Object> list;
        private HashMap<String, Object> map;

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public List<Object> getList() {
            return list;
        }

        public void setList(List<Object> list) {
            this.list = list;
        }

        public HashMap<String, Object> getMap() {
            return map;
        }

        public void setMap(HashMap<String, Object> map) {
            this.map = map;
        }
    }



    public class NonEntity implements Serializable {
        private String value;
        private Integer integer;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Integer getInteger() {
            return integer;
        }

        public void setInteger(Integer integer) {
            this.integer = integer;
        }

        @Override
        public String toString() {
            return "NonEntity{" +
                    "value='" + value + '\'' +
                    ", integer=" + integer +
                    '}';
        }
    }
```

**Attention:** please keep in mind, that you cannot store non-entities directly. Only a member variable of an entity (even if it is in a list or Map) might be non-entities.

#### complex data structures

In the jUnit tests, _Morphium_ is tested to support those complex data structures, like lists of lists, lists of maps or maps of lists of entities. I think, you'll get the picture:

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

have a look at the Tests in code on github for more examples. the main challenge here is, to determine the right type of elements in the list in order to be able to de-serialize them properly. In this case, de-serialization is done in background transparently:

```java
 @Test
    public void testListOfListOfMap() {
        morphium.dropCollection(MapListObject.class);

        CMapListObject o = new CMapListObject();
        List<List<Map<String, String>>> lst = new ArrayList<>();
        List<Map<String, String>> l2 = new ArrayList<>();
        Map<String, String> map = new HashMap<>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        l2.add(map);
        map = new HashMap<>();
        map.put("k11", "v11");
        map.put("k21", "v21");
        map.put("k31", "v31");
        l2.add(map);
        lst.add(l2);

        l2 = new ArrayList<>();
        map = new HashMap<>();
        map.put("k15", "v1");
        map.put("k25", "v2");
        l2.add(map);
        map = new HashMap<>();
        map.put("k51", "v11");
        map.put("k533", "v21");
        map.put("k513", "v31");
        l2.add(map);
        map = new HashMap<>();
        map.put("k512", "v11");
        map.put("k514", "v21");
        map.put("k513", "v31");
        l2.add(map);

        lst.add(l2);
        o.setMap7a(lst);

        morphium.store(o);

        CMapListObject ml = morphium.findById(CMapListObject.class, o.getId());
        assert (ml.getMap7a().get(1).get(0).get("k15").equals("v1"));
    }
```

as you see here, the deserialization is done transparently in background even on several levels "down", the `CMapListObject` is initialized properly.

_Caveat_: this can only work, if java knows the type of the elements in the list. As soon as there is a `List<Object>` in the type definition, morphium does not know, what the type might be. It will try to deserialize it (which will work if it is a proper entity), but might not work in all cases. If this detection fails, you'll likely end up getting a `ClassCastException`. If so, try to define the data structure more strictly or simplify it.

#### Support for MapReduce

To do complex aggregations and analysis of your data in MongoDB the first choice to do that was _MapReduce_. If necessary or convenient, you can use that with _Morphium_ as well, although it is not as powerful as the _Aggregation Framework_ (see below).

Here is a basic example on how to use _MapReduce_:

```java

    private void doSimpleMRTest(Morphium m) throws Exception {
        List<UncachedObject> result = m.mapReduce(UncachedObject.class, "function(){emit(this.counter%2==0,this);}", "function (key,values){var ret={_id:ObjectId(), value:\"\", counter:0}; if (key==true) {ret.value=\"even\";} else { ret.value=\"odd\";} for (var i=0; i<values.length;i++){ret.counter=ret.counter+values[i].counter;}return ret;}");
        assert (result.size() == 2);
        boolean odd = false;
        boolean even = false;
        for (UncachedObject r : result) {
            if (r.getValue().equals("odd")) {
                odd = true;
            }
            if (r.getValue().equals("even")) {
                even = true;
            }
            assert (r.getCounter() > 0);
        }
        assert (odd);
        assert (even);
    }
```

the problem here is, that you need to write _JavaScript_ code and hence need to switch between contexts, whereas the Aggregation support in _Morphium_ lets you define the whole pipeline in Java.

#### automatic retries on error

The write concern aka WriteSafety-Annotation in _Morphium_ is not enough for being on the safe side. the WriteSafety only makes sure, that, if all is ok, data is written to the amount of nodes, you want it to be written. You define the safety level more or less in an Application point of view. This does not affect networking outage or other problems. Also in case of a failover during access, you will end up with an exception in application. In order to deal with the problem, the coding advice for MongoDB is, to have all accesses run in a loop so that you can retry on failure and hope for fast recovery.

_Morphium_ takes care of that: all access to mongo is done in a loop and _Morphium_ tries to detect if that error is recoverable (like a failover) or not. there are several retry-settings in the config.

**retry settings in writers**
_Morphium_ has 3 different types of writers:

- the normal writer: supports asynchronous and synchronous writes
- the async writer: forces asynchronous writes
- the buffered writer: stores write requests in a buffer and executes those on block

This has some implications, as the core of _Morphium_ is asynchronous, we need to make sure, there are not too many pending writes. (the "pile" is determined by the maximum amount of connections to mongo - hence this is something you won't need to configure)
This is where the retry settings for writers come in. When writing data, this data is either written synchronously or asynchronously. In the latter case, the requests tend to pile up on heavy load. And we need to handle the case, when this pile gets too high. This is the retry. When the pile of pending requests is too high, wait for a specified amount of time and try again to queue the operation. If that fails for all retries - throw an exception.

**Retry settings for Network errors**
As we had a really sh... network which causes problems more than once a day, we needed to come up with a solution for this as well. As our network does not fail for more than a couple of requests, the idea is to detect network problems and retry the operation after a certain amount of time. This setting is specified globally in _Morphium_ config:

´´´java
morphiumConfig.setRetriesOnNetworkError(10);
morphiumConfig.setSleepBetweenNetworkErrorRetries(500);
´´´
This causes _Morphium_ to retry any operation on mongo 10 times (if a network related error occurs) and pause 500ms between each try. This includes, reads, writes, updates, index creation and aggregation. If the access failed after the (in this case) 10th try - rethrow the networking error to the caller.

## configuring _Morphium_: `MorphiumConfig`

MorphiumConfig is the class to encapsulate all settings for _Morphium_. The most obvious settings are the host seed and port definitions. But there is a ton of additional settings available.

### Different sources

#### Json

The standard `toString()`method of MorphiumConfig creates an Json String representation of the configuration. to set all configuration options from a json string, just call `createFromJson`.

#### Properties

the configuration can be stored and read from a property object.

`MorphiumConfig.fromProperties(Properties p);` Call this method to set all values according to the given properties. You also can pass the properties to the constructor to have it configured.

To get the properties for the current configuration, call `asProperties()` on a configured MorphiumConfig Object.

Here is an example property-file:

    	maxWaitTime=1000
    	maximumRetriesBufferedWriter=1
    	maxConnections=100
    	retryWaitTimeAsyncWriter=100
    	maxAutoReconnectTime=5000
    	blockingThreadsMultiplier=100
    	housekeepingTimeout=5000
    	hosts=localhost\:27017, localhost\:27018, localhost\:27019
    	retryWaitTimeWriter=1000
    	globalCacheValidTime=50000
    	loggingConfigFile=file\:/Users/stephan/_Morphium_/target/classes/_Morphium_-log4j-test.xml
    	writeCacheTimeout=100
    	connectionTimeout=1000
    	database=_Morphium__test
    	maximumRetriesAsyncWriter=1
    	maximumRetriesWriter=1
    	retryWaitTimeBufferedWriter=1000

The minimal property file would define only `hosts` and `database`. All other values would be defaulted.

If you want to specify classes in the config (like the Query Implementation), you need to specify the full qualified class name, e.g. `de.caluga.morphium.customquery.QueryImpl`

#### Java-Code

The most straight forward way of configuring _Morphium_ is, using the object directly. This means you call the getters and setters according to the given variable names above (like `setMaxAutoReconnectTime()`).

The minimum configuration is explained above: you only need to specify the database name and the host(s) to connect to. All other settings have sensible defaults, which should work for most cases.

### Configuration Options

There are a lot of settings and customizations you can do within _Morphium_. Here we discuss _all_ of them:

- _loggingConfigFile_: can be set, if you want _*Morphium*_ to configure your log4j for you. _Morphium_ itself has a dependency to log4j (see Dependencies).
- _camelCaseConversion_: if set to false, the names of your entities (classes) and fields won't be converted from camelcase to underscore separated strings. Default is `true` (convert to camelcase)
- _maxConnections_: Maximum Number of connections to be built to mongo, default is 10
- _houseKeepingTimeout_: the timeout in ms between cache housekeeping runs. Defaults to 5sec
- _globalCacheValidTime_: how long are Cache entries valid by default in ms. Defaults to 5sek
- _writeCacheTimeout_: how long to pause between buffered writes in ms. Defaults to 5sek
- _database_: Name of the Database to connect to.
- _connectionTimeout_: Set a value here (in ms) to specify how long to wait for a connection to mongo to be established. Defaults to 0 (⇒ infinite)
- _socketTimeout_: how long to wait for sockets to be established, defaults to 0 as well
- _checkForNew_: This is something interesting related to the creation of ids. Usually Ids in mongo are of type `ObjectId`. Anytime you write an object with an `_id` of that type, the document is either updated or inserted, depending on whether or not the ID is available or not. If it is inserted, the newly created ObjectId is being returned and add to the corresponding object. But if the id is not of type ObjectId, this mechanism will fail, no objectId is being created. This is no problem when it comes to new creation of objects, but with updates you might not be sure, that the object actually is new or not. If this obtion is set to `true` _*Morphium*_ will check upon storing, whether or not the object to be stored is already available in database and would update.
- _writeTimeout_: this timeout determines how long to wait until a write to mongo has to be finshed. Default is `0`⇒ no timeout
- _maximumRetriesBufferedWriter_: When writing buffered, how often should retry to write the data until an exception is thrown. Default is 10
- _retryWaitTimeBufferedWriter_: Time to wait between retries
- _maximumRetriesWriter_, _maximumRetriesAsyncWriter_: same as _maximumRetriesBufferedWriter_, but for direct storage or asynchronous store operation.
- _retryWaitTimeWriter_, _retryWaitTimeAsyncWriter_: similar to _retryWaitTimeBufferedWriter_, but for the according writing type
- _globalW_: W sets the number of nodes to have finished the write operation (according to your safe and j / fsync settings)
- _maxWaitTime_: Sets the maximum time that a thread will block waiting for a connection.
- _serverSelectionTimeout_: Defines how long the driver will wait for server selection to succeed before throwing an exception
- _writeBufferTime:_ Timeout for buffered writes. Default is 0
- _autoReconnect_: if set to `true` connections are re-established, when lost. Default is `true`
- _maxAutoReconnectTime_: how long to try to reconnect (in ms). Default is `0`⇒ try as long as it takes
- _mongoLogin_,_mongoPassword_: User Credentials to connect to MongoDB. Can be null.
- _mongoAdminUser_, _mongoAdminPwd_: Credentials to do admin tasks, like get the replicaset status. If not set, use mongoLogin instead.
- _autoValuesEnabled_: _Morphium_ supports automatic values being set to your POJO. These are configured by annotations (`@LasChange`, `@CreationTime`, `@LastAccess`, ...). If you want to switch this off _globally_, you can set it in the config. Very useful for test environments, which should not temper with productional data. By default the auto values are _enabled_.
- _readCacheEnabled_: Globally enable or disable readcache. This only affects entities with a `@Cache` annotation. By default it's enabled.
- _asyncWritesEnabled_: Globally enable or disalbe async writes. This only affects entities with a `@AsyncWrites`annotation
- _bufferedWritesEnabled_: Globally enable or disable buffered writes. This only affects entities with a `@WriteBuffer` annotation
- `defaultReadPreference`: whether to read from primary, secondary or nearest by default. Can be defined with the `@ReadPreference` annotation for each entity.
- `replicaSetMonitoringTimeout`: time interval to update replicaset status.
- _retriesOnNetworkError_: if you happen to have an unreliable network, maybe you want to retry writes / reads upon network error. This settings sets the number of retries for that case.
- _sleepBetweenNetworkErrorRetries_: set the time to wait between network error retries.
- _autoIndexAndCappedCreationOnWrite_: This setting is by default _true_ which means, that _Morphium_ keeps a list of existing collections. When a collection would be created automatically by writing to it, _Morphium_ can then and only then have all indexes and capped settings configured for that specific collection. Causes a little overhead on write access to see, if a collection exists. Probably a good idea to switch off in production environment, but for development it makes things easier.

In addition to those settings describing the behaviour of _*Morphium*_, you can also define custom classes to be used internally:

- _omClass_: here you specify the class, that should be used for mapping POJOs (your entities) to `Documnet`. By Default it uses the `ObjectMapperImpl`. Your custom implementation must implement the interface `ObjectMapper`.
- _iteratorClass_: set the Iterator implementation to use. By default `MorphiumIteratorImpl`is being used. Your custom implementation must implement the interface `MorphiumIterator`
- _aggregatorClass_: this is _*Morphium*_'s representation of the aggregator framework. This can be replaced by a custom implementation if needed. Implements `Aggregator` interface
- _aggregatorFactoryClass_: this is _*Morphium*_'s representation of the aggregator framework. This can be replaced by a custom implementation if needed. Implements `AggregatorFactory` interface
- _queryClass_ and _fieldImplClass_: this is used for Queries. If you want to take control over how queries ar built in _*Morphium*_ and on how fields within queries are represented, you can replace those two with your custom implementation.
- _queryFactoryClass_: query factory implementation, usually just creates a Query-Object. Custom implementations need to implement the `QueryFactory` interface.
- _cache_: Set your own implementation of the cache. It needs to implement the `MorphiumCache` interface. Default is `MorphiumCacheImpl`. You need to specify a fully configured cache object here, not only a class object.
- _driverClass_: Set the driver implementation, you want to use. This is a string, set the class name here. E.g. `MorphiumConfig.setDriverClass(MetaDriver.class.getName()`. Custom implementations need to implement the `MorphiumDriver` interface. By default the `MongodbDriver` is used, which connects to mongo using the official Java driver. But there are some other implementations, that do have some advantages (like the inMemoryDriver or the ones from the project [here](https://github.com/sboesebeck/morphium-drivers).

In Mongo until V 2.4 authentication and user privileges were not really existent. With 2.4, roles are introduces which might make it a bit more complicated to get things working.

### authentication

_Morphium_ supports authentication, of course, but on startup. So usually you have an application user, which connects to database. Login to mongo is configured as follows:

```java
    MorphiumConfig cfg=new Morpiumconfig(...);
    ...
    cfg.setMongoLogin("tst");
    cfg.setMongoPassword("tst");
```

This user usually needs to have read/write access to the database. If you want your indices to be created automatically by you, this user also needs to have the role _dbAdmin_ for the corresponding database. If you use _Morphium_ with a replicaset of mongo nodes, _Morphium_ needs to be able to get access to local database and get the replicaset status. In order to do so, either the mongo user needs to get additional roles (clusterAdmin and read to local db), or you specify a special user for that task, which has excactly those roles. _Morphium_ authenticates with that different user for accessing replicaSet status (and only for getting the replicaset status) and is configured very similar to the normal login:

```java
     cfg.setMongoAdminUser("adm");
     cfg.setMongoAdminPwd("adm");
```

#### corresponding MongoD Config

You need to run your mongo nodes with -auth (or authenticate = true set in config) and if you run a replicaset, those nodes need to share a key file or kerberos authentication. (see http://docs.mongodb.org/manual/reference/user-privileges/) Let's assume, that all works for now. Now you need to specify the users. One way of doing that is the following:

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

Here morphium*test is your application database \_Morphium* is connected to primarily. The admin db is a system database.

This is far away from being a complete guide, I hope this just gets you started with authentication....

## Entity Definition

Entities in _*Morphium*_ are just "Plain old Java Objects" (POJOs). So you just create your data objects, as usual. You only need to add the annotation `@Entity` to the class, to tell _*Morphium*_ "Yes, this can be stored". The only additional thing you need to take care of is the definition of an ID-Field. This can be any field in the POJO identifying the instance. Its best, to use `ObjectID` as type of this field, as these can be created automatically and you don't need to care about those as well.

If you specify your ID to be of a different kind (like String), you need to make sure, that the String is set, when the object will be written. Otherwise you might not find the object again. So the shortest Entity would look like this:

```java
	   @Entity
	    public class MyEntity {
	       @Id private ObjectId id;
	       //.. add getter and setter here
	    }
```

### indexes

Indexes are _critical_ in mongo, so you should definitely define your indexes as soon as possible during your development. Indexes can be defined on the Entity itself, there are several ways to do so: - @Id always creates an index - you can add an `@Index` to any field to have that indexed:
@Index
private String name;

you can define combined indexes using the `@Index` annotation at the class itself:

```java
		@Index({"counter, name","value,thing,-counter"}
		public class MyEntity {
```

This would create two combined indexes: one with `counter` and `name` (both ascending) and one with `value`, `thing` and descending `counter`. You could also define single field indexes using this annotations, but it's easier to read adding the annotation directly to the field.

Indexes will be created automatically if you _create_ the collection. If you want the indexes to be created, even if there is already data stores, you need to call `morphium.ensureIndicesFor(MyEntity.class)`- You also may create your own indexes, which are not defined in annotations by calling `morphium.ensureIndex()`. As parameter you pass on a Map containing field name and order (-1 or 1) or just a prefixed list of strings (like` "-counter","name"`).

Every Index might have a set of options which define the kind of this index. Like `buildInBackground` or `unique`. You need to add those as second parameter to the Index-Annotation:

```java
@Entity
@Index(value = {"-name, timer", "-name, -timer", "lst:2d", "name:text"},
	        options = {"unique:1", "", "", ""})
public static class IndexedObject {
```

here 4 indexes are created. The first two are more or less standard, wheres the `lst` index is a geospatial one and the index on `name` is a text index (only since mongo 2.6). If you need to define options for one of your indexes, you need to define it for all of them (here, only the first index is unique).

#### Text indexes

MongoDB has a built in text search functionality since V3.x. This can be used in command line, or using _Morphium_. In order for it to work, a _text index_ needs to be defined for the entity/collection. Here an example for an entity called `Person`:

```java
@Entity
    @Index(value = {"vorname:text,nachname:text,anrede:text,description:text", "age:1"}, options = {"name:myIdx"})
    public static class Person {
	    //properties and getters/setters left out for readability
    }
```

in this case, a text index was built on fields `vorname`, `nachname`, `andrede` and `description`.

To use the index, we need to create a text query[^text search and text indices can be disabled in mongoDB config. When creating the index, it would throw an Exception]:

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

In this case, there is some Data created, which puts the name of some superheroes in a mongo. Searching for the text ist something different than searching via regular expressions, because Text Indexes are way more efficient in that case.

If you need more information on text indexes, have a look at MongoDBs documentation and take a look at the Tests for TextIndexes within the source code of _Morphium_.

### capped collections

Similar as with indexes, you can define you collection to be capped using the `@Capped` annotation. This annotation takes two arguments: the maximum number of entries and the maximum size. If the collection does not exist, it will be created as capped collection using those two values. You can always ensureCapped your collection, unfortunately then only the `size` parameter will be honoured.

## Querying

Querying is done via the Query-Object, which is created by _*Morphium*_ itself (using the Query Factory). The definition of the query is done using the fluent interface:

```java
Query<MyEntity> query=_Morphium_.createQueryFor(MyEntity.class);
query=query.f("id").eq(new ObjectId());
query=query.f("valueField").eq("the value");
query=query.f("counter").lt(22);
query=query.f("personName").matches("[a-zA-Z]+");
query=query.limit(100).sort("counter");
```

In this example, I refer to several fields of different types. The Query itself is always of the same basic syntax:

```java
queryObject=queryObject.f(FIELDNAME).OPERATION(Value);
queryObject=queryObject.skip(NUMBER); //skip a number of entreis
queryObject=queryObject.limig(NUMBER); // limit result
queryObject.sort(FIELD_TO_SORTBY);`
```

As field name you may either use the name of the field as it is in mongo or the name of the field in java. If you specify an unknown field to _*Morphium*_, a `RuntimeException` will be raised.

For definition of the query, it's also a good practice to define enums for all of your fields. This makes it hard to have mistypes in a query:

```java
		public class MyEntity {
		      //.... field definitions
		      public enum Fields { id, value, personName,counter, }
		}
```

There is a IntelliJ plugin ("GeneratePropertyEnums") that is used for creating those enums automatically. Then, when defining the query, you don't have to type in the name of the field, just use the field enum:

`query=query.f(MyEntity.Fields.counter).eq(123);`

This avoids typos and shows compile time errors, when a field was renamed for whatever reason.

After you defined your query, you probably want to access the data in mongo. Via _*Morphium*_,there are several possibilities to do that: - `queryObject.get()`: returns the first object matching the query, only one. Or null if nothing matched - `queryObject.asList()`: return a list of all matching objects. Reads all data in RAM. Useful for small amounts of data - `Iterator<MyEntity> it=queryObject.asIterator()`: creates a `MorphiumIterator` to iterate through the data, which does not read all data at once, but only a couple of elements in a row (default 10).

### Simple queries

most of your queries probably are simple ones. like searching for a special id or value. This is done rather simply with the query-Object: morphium.createQueryFor(MyEntity.class).f("field").eq(value) if you add more f(fields) to the query, they will be concatenated by a logical AND. so you can do something like:

```java
    Query<UncachedObject> q=morphium.createQueryFor(UncachedObject.class);
    q.f("counter").gt(10).f("counter").lt(20);
```

This would result in a query like: "All Uncached Objects, where counter is greater than 10 and counter is less then 20".

### Or Queries

in addition to those AND-queries you can add an unlimited list of queries to it, which will be concatenated by a logical OR.

```java
   q.f("counter").lt(100).or(q.q().f("value").eq("Value 12"), q.q().f("value").eq("other"));
```

This would create a query like: "all UncachedObjects where counter is less than 100 and (value is 'value 12' or value is 'other')"

the Method q() creates a new empty query for the same object. It's a convenience Method. Please be careful, never use your query Object in the parameter list of or - this would cause and endless loop! ATTENTION here!

This gives you the possibility to create rather complex queries, which should handle about 75% of all cases. Although you can also add some NOR-Queries as well. These are like "not or"-Queries....

```java
   q.f("counter").lt(100).nor(q.q().f("counter").eq(90), q.q().f("counter").eq(55));
```

this would result in a query like: "All query objects where counter is less than 100 and not (counter=90 or counter=55).

this adds another complexity level to the queries ;-)

If that's not enough, specify your own query in "mongo"-Syntax.
You can also specify your own query object (Map<String,Object>) in case of a very complex query. This is part of the Query-Object and can be used rather easily:

```java
        Map<String,Object> query=new HashMap<>();
        query.put("counter",UtilsMap.of("$lt",10));
        Query<UncachedObject> q=morphium.createQueryFor(UncachedObject.class);
        List<UncachedObject> lst=q.complexQuery(query);
```

Although, in this case the query is a very simple one (counter < 10), but I think you get the Idea....

#### Limitations

Well, the fluent query interface does have its limitations. So its not possible to have a certain number of or-concatenated queries (like (counter==14 or Counter <10) and (counter >50 or counter ==30)). I'm not sure, this is very legible...

### the Iterator

_*Morphium*_ has support for a special Iterator, which steps through the data, a couple of elements at a time. By Default this is the standard behaviour. But the \__Morphium_\_Iterator ist quite capable:

- `queryObject.asIterable()` will stepp through the result list, 10 at a time
- `queryObject.asIterable(100)` will step through the result list, 100 at a time
- `queryObject.asIterable(100,5)` will step through the result list, 100 at a time and keep 4 chunks of 100 elements each as prefetch buffers. Those will be filled in background.
- `MorphiumIterator it=queryObject.asIterable(100,5); it.setmultithreadedAccess(true);` use the same iterator as before, but make it thread safe.

**Description**
Problem is, when dealing with huge tables or lots of data, you'd probably include paging to your queries. You would read data in chunks of for example 100 objects to avoid memory overflows. This is now available by _Morphium_. The new MorphiumIterator works as Iterable or Iterator - whatever you like. It's included in the Query-interface and can be used very easily:

```java
Query<Type> q=morphium.createQueryFor(Type.class);
q=q.f("field").eq..... //whatever

for (Type t:q.asIterable()) {
   //do something with t
}
```

This creates an iterator, reading all objects from the query in chunks of 10... if you want to read them one by one, you only ned to give the chunk-size to the call:

```java
for (Type t:q.asIterable(1)) {
   //now reads every single Object from db
}
```

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
            morphium.store(uc);
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

**Advanced Features**

Since V2.2.5 the _Morphium_ iterator supports lookahead (prefetching). This means its not only possible to define a window size to step through your data, but also how many of those windows should be prefetched, while you step through the first one.

This works totally transparent for the user, its just a simple call to activate this feature:

```java
theQuery.asIterable(1000,5); //window size 1000, 5 windows prefetch
```

Since 2.2.5 the _Morphium_ iterator is also able to be used by multiple threads simultaneously. This means, several threads access the _same_ iterator. This might be useful for querying and alike.
To use that, you only need to set `setMultithreaddedAccess` to true in the iterator itself:

```java
MorphiumIterator<MyEntity> it=theQuery.asIterable(1000,15)
it.setMultithreaddedAccess(true);
```

_Attention_: Setting mutlithreaddedAccess to true will cause the iterator to be a bit slower as it has to do some things in a `synchronized` fashion.

## Storing

Storing is more or less a very simple thing, just call `_Morphium_.store(pojo)` and you're done. Although there is a bit more to it: - if the object does not have an id (id field is `null`), there will be a new entry into the corresponding collection. - if the object does have an id set (!= `null`), an update to db is being issued. - you can call `_Morphium_.storeList(lst)` where lst is a list of entities. These would be stored in bulkd, if possible. Or it does a bulk update of things in mongo. Even mixed lists (update and inserts) are possible. _*Morphium*_ will take care of sorting it out - there are additional methods for writing to mongo, like update operations `set`, `unset`, `push`, `pull` and so on (update a value on one entity or for all elements matching a query), `delete` objects or objects matching a query, and a like - The writer that acutally writes the data, is chosen depending on the configuration of this entity (see Annotations below)

### Names of entities and fields

_*Morphium*_ by defaults converts all java CamelCase identifiers in underscore separated strings. So, `MyEntity` will be stored in an collection called `my_entity` and the field `aStringValue` would be stored in as `a_string_value`.

When specifying a field, you can always use either the transformed name or the name of the corresponding java field. Collection names are always determined by the classname itself.

#### CamelCase conversion

But in _*Morphium*_ you can of course change that behaviour. Easiest way is to switch off the transformation of CamelCase globally by setting `camelCaseConversionEnabled` to false (see above: Configuration). If you switch it off, its off completely - no way to do switch it on for just one collection or so.

If you need to have only several types converted, but not all, you have to have the conversion globally enabled, and only switch it off for certain types. This is done in either the `@Entity` or `@Embedded` annotation.

```java
		@Entity(convertCamelCase=false)
		public class MyEntity {
		     private String myField;
```

This example will create a collection called `MyEntity` (no conversion) and the field will be called `myField` in mongo as well (no conversion).

_Attention_: Please keep in mind that, if you switch off camelCase conversion globally, nothing will be converted!

#### using the full qualified classname

you can tell _*Morphium*_ to use the full qualified classname as basis for the collection name, not the simple class name. This would result in createing a collection `de_caluga_morphium_my_entity` for a class called `de.caluga.morphium.MyEntity`. Just set the flag `useFQN` in the entity annotation to `true`.

```java
		@Entity(useFQN=true)
		public class MyEntity {
```

Recommendation is, not to use the full qualified classname unless it's really needed.

#### Specifying a collection / fieldname

In addition to that, you can define custom names of fields and collections using the corresponding annotation (`@Entity`, `@Property`).

For entities you may set a custom name by using the `collectionName` value for the annotation:

```java
		@Entity(collectionName="totallyDifferent")
		public class MyEntity {
		    private String myValue;
		}
```

the collection name will be `totallyDifferent` in mongo. Keep in mind that camel case conversion for fields will still take place. So in that case, the field name would probably be `my_value`. (if camel case conversion is enabled in config)

You can also specify the name of a field using the property annotation:

```java
		@Property(fieldName="my_wonderful_field")
		private String something;
```

Again, this only affects this field (in this case, it will be called `my_wondwerful_field` in mongo) and this field won't be converted camelcase. This might cause a mix up of cases in your MongoDB, so please use this with care.

#### Accessing fields

When accessing fields in _*Morphium*_ (especially for the query) you may use either the name of the Field in Java (like myEntity) or the converted name depending on the config (camelCased or not, or custom).

#### Using NameProviders

In some cases it might be necessary to have the collection name calculated dynamically. This can be achieved using the `NameProvider` Interface.

You can define a NameProvider for your entity in the `@Entity` annotation. You need to specify the type there. By default, the NameProvider for all Entities is `DefaultNameProvider`. Which actually looks like this:

```java
public final class DefaultNameProvider implements NameProvider {

	@Override
	public String getCollectionName(Class<?> type, ObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, _Morphium_ _Morphium_) {

	    String name = type.getSimpleName();

	    if (useFQN) {
	        name = type.getName().replaceAll("\\.", "_");
	    }
	    if (specifiedName != null) {
	        name = specifiedName;
	    } else {
	        if (translateCamelCase) {
	            name = _Morphium_.getARHelper().convertCamelCase(name);
	        }
	    }
	    return name;
	}
}
```

You can use your own provider to calculate collection names depending on time and date or for example depending on the querying host name (like: create a log collection for each server separately or create a collection storing logs for only one month each).
**Attention**: Name Provider instances will be cached, so please implement them thread safe.

#### examples

mongo is really fast and stores a lot of date in no time. Sometimes it's hard then, to get this data out of mongo again, especially for logs this might be an issue (in our case, we had more than a 100 million entries in one collection). It might be a good idea to change the collection name upon some rule (by date, timestamp whatever you like). _Morphium_ supports this using a strategy-pattern.

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

This would create a monthly named collection like "my_entity_201206". In order to use that name provider, just add it to your `@Entity`-Annotation:

```java
@Entity(nameProvider = DatedCollectionNameProvider.class)
public class MyEntity {
....
}
```

**performance**:
The name provider instances themselves are cached for each type upon first use, so you actually might do as much work as possible in the constructor.
BUT: on every read or store of an object the corresponding name provider method `getCollectionName` is called, this might cause Performance drawbacks, if you logic in there is quite heavy and/or time consuming.

### Automatic values

This is something quite common: you want to know, when your data was last changed and maybe who did it. Usually you keep a timestamp with your object and you need to make sure, that these timestamps are updated accordingly. _Morphium_ does this automatically - just declare the annotations:

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

_Explanation:_

`@LastAccess`: Stores the last time, this object was read from db! Careful with that one: it will create a write access, for _every read_!
`@CreationTime`: Stores the creation timestamp
`@LastChange`: Timestamp the last moment, this object was stored.

## Asynchronous API

All writer implementation support asynchronous calls like

```java
   public <T> void store(List<T> lst, AsyncOperationCallback<T> callback);
```

if callback==null the method call should be synchronous... If callback!=null do the call to mongo asynchronous in background. Usually, you specify the default behaviour in your class definition:

```java
  @Entity
  @AsyncWrites
  public class EntityType {
   ...
  }
```

All write operations to this type will be asynchronous! (synchronous call is not possible in this case!).

Asynchronous calls are also possible for Queries, you can call q.asList(callback) if you want to have this query be executed in background.

### Difference asynchronous write / write buffer

Asynchronous calls will be issued at once to the mongoDb but the calling thread will not have to wait. It will be executed in Background. the `@WriteBuffer` annotation specifies a write buffer for this type (you can specify the size etc if you like). All writes will be held temporarily in ram until time frame is reached or the number of objects in write buffer exceeds the maximum you specified (0 means no maximum). Attention if you shut down the Java VM during that time, those entries will be lost. Please only use that for logging or "not so important" data. specifying a write buffer four you entitiy is quite easy:

```java
  @Entity
  @WriteBuffer(size=1000, timeout=5000)
  public class MyBufferedLog {
  ....
  }
```

This means, all write access to this type will be stored for 5 seconds or 1000 entries, whichever occurs first. If you want to specify a different behavior when the maximum number of entries is reached, you can specify a strategy:

- `WRITE_NEW`: write newest entry (synchronous and not add to buffer)
- `WRITE_OLD`: write some old entries (and remove from buffer)
- `DEL_OLD`: delete old entries from buffer - oldest elements won't be written to Mongo!
- `IGNORE_NEW`: just ignore incoming - newest elements WILL NOT BE WRITTEN!
- `JUST_WARN`: increase buffer and warn about it

## Validation support

Morphium does support for javax.validation annotations and those might be used to ensure data quality:

```java
 @Id
    private MorphiumId id;

    @Min(3)
    @Max(7)
    private int theInt;

    @NotNull
    private Integer anotherInt;

    @Future
    private Date whenever;

    @Pattern(regexp = "m[ueü]nchen")
    private String whereever;

    @Size(min = 2, max = 5)
    private List friends;

    @Email
    private String email;
```

You do not need to have any validator implementation in classpath, _Morphium_ detects, if validation is available and only enables it then.

## Annotations

a lot of things can be configured in _*Morphium*_ using annotations. Those annotations might be added to either classes, fields or both.

### Entity

Perhaps _the_ most important Annotation, as it has to be put on every class the instances of which you want to have stored to database. (Your data objects).

By default, the name of the collection for data of this entity is derived by the name of the class itself and then the camel case is converted to underscore strings (unless config is set otherwise).

These are the settings available for entities:

- `translateCamelCase`: default true. If set, translate the name of the collection and all fields (only those, which do not have a custom name set)
- `collectionName`: set the collection name. May be any value, camel case won't be converted.
- `useFQN`: if set to true, the collection name will be built based on the full qualified class name. The Classname itself, if set to false. Default is false
- `polymorph`: if set to true, all entities of this type stored to mongo will contain the full qualified name of the class. This is necessary, if you have several different entities stored in the same collection. Usually only used for polymorph lists. But you could store any polymorph marked object into that collection Default is false
- `nameProvider`: specify the class of the name provider, you want to use for this entity. The name provider is being used to determine the name of the collection for this type. By Default it uses the `DefaultNameProvider` (which just uses the classname to build the collection name). see above

### Embedded

Marks POJOs for object mapping, but don't need to have an ID set. These objects will be marshalled and un-marshalled, but only as part of another object (Subdocument). This has to be set at class level.

You can switch off camel case conversion for this type and determine, whether data might be used polymorph.

### AsyncWrites

ensures, that all write accesses to this entity are asynchronous.

### NoCache

switches OFF caching for this entity. This is useful if some superclass might have caches enabled and we need to disable it here.

### Capped

Valid at: Class level
Tells _*Morphium*_ to create a capped collection for this object (see capped collections above).
Parameters:

- _maxSize_: maximum size in byte. Is used when converting to a capped collection
- _maxNumber_: number of entries for this capped collection

### Collation

These are the collation settings for this given entity. will be used when creating new collections and indices

### AdditionalData

Special feature for _*Morphium*_: this annotation has to be added for at lease _one_ field of type Map<String,Object>. It does make sure, that all data in Mongo, that cannot be mapped to a field of this entity, will be added to the annotated Map properties.

by default this map is read only. But if you want to change those values or add new ones to it, you can set `readOnly=false`.

### Aliases

It's possible to define aliases for field names with this annotation (hence it has to be added to a field).

```java
@Alias({"stringList","string_list"})
List<String> strLst;
```

in this case, when reading an object from MongoDB, the name of the field `strLst` might also be `stringList` or `string_list` in mongo. When storing it, it will always be stored as `strLst` or `str_lst` according to configs camelcase settings.

This feature comes in handy when migrating data.

### CreationTime

has to be added to both the class and the field(s) to store the creation time in. This value is set in the moment, the object is being stored to mongo. The data type for creation time might be:

- `long` / `Long`: store as timestamp
- `Date`: store as date object
- `String`: store as a string, you may need to specify the format for that

### LastAccess

same as creation time, but storing the last access to this type. **Attention**: will cause all objects read to be updated and written again with a changed timestamp.

Usage: find out, which entries on a translation table are not used for quite some time. Either the translation is not necessary anymore or the corresponding page is not being used.

### LastChange

Same as the two above, except the timestamp of the last change (to mongo) is being stored. The value will be set, just before the object is written to mongo.

### DefaultReadPreference

Define the read preference level for an entity. This annotation has to be used at class level. Valid types are:

- `PRIMARY`: only read from primary node
- `PRIMARY_PREFERED`: if possible, use primary.
- `SECONDARY`: only read from secondary node
- `SECONDARY_PREFERED`: if possible, use secondary
- `NEAREST`: I don't care, take the fastest

### Id

Very important annotation to a field of every entity. It marks that field to be the id and identify any object. It will be stored as `_id` in mongo (and will get an index).

The Id may be of any type, though usage of ObjectId is strongly recommended.

### Index

Define indexes. Indexes can be defined for a single field. Combined indexes need to be defined on class level. See above.

### IgnoreFields

List of fields in class, that can be ignored. Defaults no none.
usually an exact match, but can use ~ as substring, / as regex marker

Field names are JAVA Fields, not translated ones for mongo

`IgnoreFields` will not be honored for fields marked with `@Property` and a custom fieldname

this will be inherited by subclasses!

```java

   @Entity
   @IgnoreFields({"var1", "var3"})
   public class TestClass {
       @Id
       public MorphiumId id;
       public int var1;
       public int var2;
       public int var3;
   }
```

### LimitToFields

this is a positive list of fields to use for MongoDB. All fields, not listed here will be ignored when it comes to mongodb.

```java
 @Entity
    @LimitToFields({"var1"})
    public class TestClass2 {
        @Id
        public MorphiumId id;
        public int var1;
        public int var2;
        public int var3;
    }
```

`LimitToFields` also takes a Class as an argument, then the fields will be limited to the fields of the given class.

```java

    @Entity
    @LimitToFields(type = TestClass2.class)
    public class TestClass3 extends TestClass2 {

        public String notValid;
    }

```

### Property

Can be added to any field. This not only has documenting character, it also gives the opportunity to change the name of this field by setting the `fieldName` value. By Default the fieldName is ".", which means "fieldName based".

### ReadOnly

Mark an entity to be read only. You'll get an exception when trying to store.

### Version

Mark a field to keep the current Version number. Field needs to be of type Long!

### Reference

If you have a member variable, that is a POJO and not a simple value, you can store it as reference to a different collection, if the POJO is an Entity (and only if!).

This also works for lists and Maps. Attention: when reading Objects from disk, references will be de-referenced, which will result into one call to mongo each.

Unless you set `lazyLoading` to true, in that case, the child documents will only be loaded when accessed.

#### Lazy Loaded references

_Morphium_ supports lazy loading of references. This is easy to use, just add `@Reference(lazyLoading=true)` to the reference you want to have them loaded lazyly.

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

When a reference is being lazy loaded, the corresponding field will be set with a Proxy for an instance of the correct type, where only the ObjectID is set. Any access to it will be catched by the proxy, and any method will cause the object to be read from DB and deserialized. Hence this object will only be loaded upon first access.

It should be noted that when using `Object.toString();` for testing that the object will be loaded from the database and appear to not be lazy loaded. In order to test Lazy Loading you should load the base object with the lazy reference and access it directly and it will be null. Additionally the referenced object will be null until the references objects fields are accessed.

### Transient

Do not store the field - similar to `@IgnoreFields` or `@LimitToFields`

### Cache

Cache settings for this entity, see the chapter about transparent caching above for more details.

### Encrypted

Encryption settings for this field. See chapter about field encryption for details

### UseIfNull

Usually, _Morphium_ does not store null values. That means, the corresponding document just would not contain the given field(s) at all.

Sometimes that might cause problems, so if you add `@UseIfNull` to any field, it will be stored into mongo even if it is null.

### LifeCycle

this annotation for an Entity tells morphium, that this entity does have some lifecycle methods defined. Those methods all need to be marked with the corresponding annotation:

- `@PostLoad`
- `@PostRemove`
- `@PostStore`
- `@PostUpdate`
- `@PreRemove` - may throw a `MorphiumAccessVetoException` to abort the removal
- `@PreStore` - may throw a `MorphiumAccessVetoException` to abort store
- `@PreUpdate` - may throw a `MorphiumAccessVetoException` to abort update

the methods where those annotations are added must not have any parameters. They should only access the local object/entity.

### Version

only used auto-versioning is enabled in `@Entity`. Defines the field to hold the version number.

### WriteSafety

Specify the safety for this entity when it comes to writing to mongo. This can range from "NONE" to "WAIT FOR ALL SLAVES". Here are the available settings:

- timeout: set a timeout in ms for the operation - if set to 0, unlimited (default). If set to negative value, wait relative to replication lag
- level: set the safety level:
  - `IGNORE_ERRORS` None, no checking is done
  - `NORMAL` None, network socket errors raised
  - `BASIC` Checks server for errors as well as network socket errors raised
  - `WAIT_FOR_SLAVE` Checks servers (at lease 2) for errors as well as network socket errors raised
  - `MAJORITY` Wait for at least 50% of the slaves to have written the data
  - `WAIT_FOR_ALL_SLAVES`: waits for all slaves to have committed the data. This is depending on how many slaves are available in replica set. Wise timeout settings are important here. See WriteConcern in MongoDB Java-Driver for additional information

#### Cluster awareness

_Morphium_ is tracking the cluster status internally in order to react properly on different scenarios[^can be switched off in morphiumConfig]. For example, if one node goes down, waiting for all nodes to write the data will result in the application blocking until the last cluster member came back up again.
This is defined by the `w`-Setting in `WriteSafety`. In a nutshell, it tells mongo on how many cluster nodes you want to have written, and will wait until this number is reached.

This caused _major_ problems with our environments, like having different cluster configurations in test and production environments.

_Morphium_ fixes that issue in that way, that when "WAIT*FOR_ALL_SLAVES" is defined in `WriteSafety`, it will set the `w`-value according to the number of \_available* slaves, resulting in no blocking. [^as it takes some time for _Morphium_ and mongo do determine if a cluster member is down, some requests might actually block]

### Annotation Inheritance

By default, Java does not support the inheritance of annotations. This is ok in most cases, but in the case of entities it's a bugger. We added inheritance to _Morphium_ to be able to build flexible data structures and store them to mongo.

### Implementation

Well, it's quite easy, actually ;-) The algorithm for getting the inherited annotations looks as follows (simplified)

1. Take the annotations from the current class, if found, return it
2. Take the superclass, if superclass is "Object" return null
3. if there is the annotation to look for, return it
4. continue with step 1

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

## Changestream support

MongoDB introduced a feature called changestreams with V4.0 of mongodb. This is a special search that returns all changes to a database or collection. This is very useful if you want to be notified about changes to certain types or about certain commands being run.

Changestreams are only available when connected to a replicaset.

_Morphium_ does support changestreams, in fact the messaging subsystem is built completely relying on this feature.

The easiest way to use changestreams is to use _Morphiums_ `ChangeStreamMonitor`:

```java
ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, UncachedObject.class);
m.start();
final AtomicInteger cnt = new AtomicInteger(0);

m.addListener(evt -> {
    printevent(evt);
    cnt.set(cnt.get() + 1);
    return true;
});
Thread.sleep(1000);
for (int i = 0; i < 100; i++) {
    morphium.store(new UncachedObject("value " + i, i));
}
Thread.sleep(5000);
m.terminate();
assert (cnt.get() >= 100 && cnt.get() <= 101) : "count is wrong: " + cnt.get();
morphium.store(new UncachedObject("killing", 0));

```

The monitor by definition runs asynchronous, it uses the `watch` methods to database or collection.

- `morphium.watch(Class type, boolean updateFullDocument,ChangeStreamListener lst)`: this watches in a _synchronous_ call for any change event. This call _blocks!_ until the Listener returns `false`
- `morphium.watchAsync(...)` (same parameters as above), runs asynchronously. _attention_: the Settings for asyncExcecutor in `MorphiumConfig` might affect the behaviour of this call.

There are also methods for watching _all_ changes, that happen in the connected database. This might result in a lot of callbacks: `watchDB()` and `watchDBAsync()`.

#### OplogMonitor

there is also an older implementation of this, the `OplogMonitor`. This one does more or less the same thing as the `ChangeStreamMonitor`, but also runs with older installations of MongoDB (when connected to a ReplicaSet).
You'd probably want to use the `ChangestreamListener` instead, as it is more efficient.

```java
OplogListener lst = data -> {
            log.info(Utils.toJsonString(data));
            gotIt = true;
        };
        OplogMonitor olm = new OplogMonitor(morphium);
        olm.addListener(lst);
        olm.start();

        Thread.sleep(100);
        UncachedObject u = new UncachedObject("test", 123);
        morphium.store(u);

        Thread.sleep(1250);
        assert (gotIt);
        gotIt = false;

        morphium.set(u, UncachedObject.Fields.value, "new value");
        Thread.sleep(550);
        assert (gotIt);
        gotIt = false;

        olm.removeListener(lst);
        u = new UncachedObject("test", 123);
        morphium.store(u);
        Thread.sleep(200);
        assert (!gotIt);


        olm.stop();
```

### partial updating

The idea behind partial updates is, that only the changes to an entity are transmitted to the database and will thus reduce the load on network and MongoDB itself.

This is the easiest way - you already know, what fields you changed and maybe you even do not want to store fields, that you actually did change. In that case, call the updateUsingFields-Method:

```java
   UncachedObject o....
   o.setValue("A value");
   o.setCounter(105);
   Morphium.get().updateUsingFields(o,"value");
         //does only send updates for Value to mongodb
         //counter is ignored
```

`updateUsingFields()` honours the lifecycle methods as well as caches (write cache or clear read_cache on write). take a look at some code from the corresponding JUnit test for better understanding:

```java
UncachedObject o... //read from MongoDB
o.setValue("Updated!");
morphium.updateUsingFields(o, "value");
log.info("uncached object altered... look for it");
Query<UncachedObject> c=morphium.createQueryFor(UncachedObject.class);
UncachedObject fnd= (UncachedObject) c.f("_id").eq( o.getMongoId()).get();
assert(fnd.getValue().equals("Updated!")):"Value not changed? "+fnd.getValue();
```

### BulkRequest support

If you need to send a lot of write requests to MongoDB, it might be useful to use _bulk requests_ for that. MongoDB does have support for that. It means, that not each command is sent on its own, but all are sent in one single bulk command to the database, which is a lot more efficient.

To use that via _Morphium_ you need to add your requests to the `BulkRequestContext`:

```java
MorphiumBulkContext c = morphium.createBulkRequestContext(UncachedObject.class, false);
c.addSetRequest(morphium.createQueryFor(UncachedObject.class).f("counter").gte(0), "counter", 999, true, true);
//could add more requests here
Map<String, Object> ret = c.runBulk();
```

There are all basic operations you might send in a bulk:

- insert
- delete
- set/unset
- inc/dec
- update
- mul (multiplication)
- ...

If there is a special request, where there is no direct support in bulk context, use the generic method `addCustomUpdateRequest()` for adding a request. You need to pass on your requests Map-Representation.

### Transaction support

MongoDB does have support for transactions in newer releases. _Morphium_ does support that as well:

```java
  @Test
    public void transactionTest() throws Exception {
        for (int i = 0; i < 10; i++) {
            try {
                morphium.createQueryFor(UncachedObject.class).delete();
                Thread.sleep(100);
                TestEntityNameProvider.number.incrementAndGet();
                log.info("Entityname number: " + TestEntityNameProvider.number.get());
                createUncachedObjects(10);
                Thread.sleep(100);


                morphium.startTransaction();
                Thread.sleep(100);
                log.info("Count after transaction start: " + morphium.createQueryFor(UncachedObject.class).countAll());
                UncachedObject u = new UncachedObject("test", 101);
                morphium.store(u);
                Thread.sleep(100);
                long cnt = morphium.createQueryFor(UncachedObject.class).countAll();
                if (cnt != 11) {
                    morphium.abortTransaction();
                    assert (cnt == 11) : "Count during transaction: " + cnt;
                }

                morphium.inc(u, "counter", 1);
                Thread.sleep(100);
                u = morphium.reread(u);
                assert (u.getCounter() == 102);
                morphium.abortTransaction();
                Thread.sleep(100);
                cnt = morphium.createQueryFor(UncachedObject.class).countAll();
                u = morphium.reread(u);
                assert (u == null);
                assert (cnt == 10) : "Count after rollback: " + cnt;
            } catch (Exception e) {
                log.error("ERROR", e);
                morphium.abortTransaction();
            }
        }

    }
```

Internally, _Morphium_ uses the transaction context if _this thread_ started a transaction (if you need a transaction spanning over Threads, you need to pass on the current transaction session:

```java
ctx=morphium.getDriver().getTransactionContext();
...
//other thread
morphium.getDriver().setTransactionContext(ctx);
```

**Caveat**: mongoDB does not support nested transactions (yet), so you will get an `Exception` when trying to start another transaction in the same thread.

## Listeners in _Morphium_

there are a lot of listeners in _Morphium_ that help you get informed about what is going on in the system. Some of which also might help you, to adapt behaviour according to your needs:

### ReplicasetStatusListener

_Morphium_ is monitoring the status of the replicaset it is connected to (default is every 5s, but can be changed in MorphiumConfigs setting `replicaSetMonitoringTimeout`). You can get this information on demand, by calling `morphium.getReplicasetStatus()`.
But you can also be informed whenever there is a change in the cluster by implementing the interface (since _Morphium_ V4.2):

```java
public interface ReplicasetStatusListener {

     void gotNewStatus(Morphium morphium, ReplicaSetStatus status);

    /**
     * infoms, if replicaset status could not be optained.
     * @param numErrors - how many errors getting the status in a row we already havei
     */
    void onGetStatusFailure(Morphium morphium, int numErrors);

    /**
     * called, if the ReplicasetMonitor aborts due to too many errors
     * @param numErrors - number of errors occured
     */
    void onMonitorAbort(Morphium morphium, int numErrors);

    /**
     *
     * @param hostsDown - list of hostnamed not up
     * @param currentHostSeed - list of currently available replicaset members
     */
    void onHostDown(Morphium morphium, List<String> hostsDown,List<String> currentHostSeed);
}
```

The `ReplicasetStatus` does contain a lot of information about the replicaset itself:

```java
public class ReplicaSetStatus {
    private String set;
    private String myState;
    private String syncSourceHost;
    private Date date;
    private int term;
    private int syncSourceId;
    private long heartbeatIntervalMillis;
    private int majorityVoteCount;
    private int writeMajorityCount;
    private int votingMembersCount;
    private int writableVotingMembersCount;
    private long lastStableRecoveryTimestamp;
    private List<ReplicaSetNode> members;
    private Map<String,Object> optimes;
    private Map<String,Object> electionCandidateMetrics;
}


public class ReplicaSetNode {
    private int id;
    private String name;
    private double health;
    private int state;
    @Property(fieldName = "stateStr")
    private String stateStr;
    private long uptime;
    @Property(fieldName = "optimeDate")
    private Date optimeDate;

    @Property(fieldName = "lastHeartbeat")
    private Date lastHeartbeat;
    private int pingMs;
    private String syncSourceHost;
    private int syncSourceId;
    private String infoMessage;
    private Date electionDate;
    private int configVersion;
    private int configTerm;
    private String lastHeartbeatMessage;
    private boolean self;
}

```

See mongoDB documentation of [`rs.status()` command](https://docs.mongodb.com/manual/reference/method/rs.status/) for more information on the different fields.

## CacheListener

Via this interface, you will be informed about cache operations and may interfere with them or change the behaviour:

```java
public interface CacheListener {
    /**
     * ability to alter cached entries or avoid caching overall
     *
     * @param toCache - datastructure containing cache key and result
     * @param <T>     - the type
     * @return false, if not to cache
     */
     //return the cache entry to be stored, null if not
    <T> CacheEntry<T> wouldAddToCache(Object k, CacheEntry<T> toCache, boolean updated);

		//return false, if you do not want cache to be cleared
    <T> boolean wouldClearCache(Class<T> affectedEntityType);

		//return false, if you do not want entry to be removed from cache
    <T> boolean wouldRemoveEntryFromCache(Object key, CacheEntry<T> toRemove, boolean expired);

}
```

### CacheSyncListener

This are special cache listeners which will be informed, when a cache needs to be updated because of incoming clear or update requests. There are two direct sub-interfaces:

- `WatchingCacheSyncListener`: to be used with `WatchingCacheSynchronizer`
- `MessagingCacheSyncListener`: to be used with `MessagingCacheSynchronizer`

The base interface is CacheSyncListener:

```java
public interface CacheSyncListener {
    /**
     * before clearing cache - if cls == null whole cache
     * Message m contains information about reason and stuff...
     */
    @SuppressWarnings("UnusedParameters")
    void preClear(Class cls) throws CacheSyncVetoException;

    @SuppressWarnings("UnusedParameters")
    void postClear(Class cls);
}
```

and the subclasses `WatchingCacheSyncListener` (just adds one other method):

```java
public interface WatchingCacheSyncListener extends CacheSyncListener {
    void preClear(Class<?> type, String operation);

}
```

and the `MessagingCacheSyncListener` which adds some Messaging based methods:

```java
public interface MessagingCacheSyncListener extends CacheSyncListener {

    /**
     * Class is null for CLEAR ALL
     *
     * @param cls
     * @param m   - message about to be send - add info if necessary!
     * @throws CacheSyncVetoException
     */
    @SuppressWarnings("UnusedParameters")
    void preSendClearMsg(Class cls, Msg m) throws CacheSyncVetoException;

    @SuppressWarnings("UnusedParameters")
    void postSendClearMsg(Class cls, Msg m);
}

```

### ChangeStreamListener

As already mentioned, this listener is used to be informed about changes in your data.

```java
public interface ChangeStreamListener {
    /**
     * return true, if you want to continue getting events.
     *
     * @param evt
     * @return
     */
    boolean incomingData(ChangeStreamEvent evt);
}
```

### MessageListener

This one is one of the core functionalities of _Morphium_ messaging, this is the placed to be informed about incoming messages:

```java
public interface ChangeStreamListener {
    /**
     * return true, if you want to continue getting events.
     *
     * @param evt
     * @return
     */
    boolean incomingData(ChangeStreamEvent evt);
}
```

### MorphiumStorageListener

If you add a listener for these kind of events, you will be informed about _any_ store via morphium. This is kind of the same thing as the `LifeCycle` annotation and the corresponding methods. But its a different design pattern. If a `MorphiumAccessVetoException` is thrown, the corresponding action is aborted.

```java
public interface MorphiumStorageListener<T> {
    void preStore(Morphium m, T r, boolean isNew) throws MorphiumAccessVetoException;

    void preStore(Morphium m, Map<T, Boolean> isNew) throws MorphiumAccessVetoException;

    @SuppressWarnings("UnusedParameters")
    void postStore(Morphium m, T r, boolean isNew);

    @SuppressWarnings("UnusedParameters")
    void postStore(Morphium m, Map<T, Boolean> isNew);

    @SuppressWarnings("UnusedParameters")
    void preRemove(Morphium m, Query<T> q) throws MorphiumAccessVetoException;

    @SuppressWarnings({"EmptyMethod", "UnusedParameters"})
    void preRemove(Morphium m, T r) throws MorphiumAccessVetoException;

    @SuppressWarnings("UnusedParameters")
    void postRemove(Morphium m, T r);

    @SuppressWarnings("UnusedParameters")
    void postRemove(Morphium m, List<T> lst);

    @SuppressWarnings("UnusedParameters")
    void postDrop(Morphium m, Class<? extends T> cls);

    @SuppressWarnings("UnusedParameters")
    void preDrop(Morphium m, Class<? extends T> cls) throws MorphiumAccessVetoException;

    @SuppressWarnings("UnusedParameters")
    void postRemove(Morphium m, Query<T> q);

    @SuppressWarnings({"EmptyMethod", "UnusedParameters"})
    void postLoad(Morphium m, T o);

    @SuppressWarnings({"EmptyMethod", "UnusedParameters"})
    void postLoad(Morphium m, List<T> o);

    @SuppressWarnings("UnusedParameters")
    void preUpdate(Morphium m, Class<? extends T> cls, Enum updateType) throws MorphiumAccessVetoException;

    @SuppressWarnings("UnusedParameters")
    void postUpdate(Morphium m, Class<? extends T> cls, Enum updateType);

    enum UpdateTypes {
        SET, UNSET, PUSH, PULL, INC, @SuppressWarnings("unused")DEC, MUL, MIN, MAX, RENAME, POP, CURRENTDATE, CUSTOM,
    }

}
```

### OplogListener

there is a listener / watch functionality that works with older Mongodb installations. The OpLogListener is used by the `OplogMonitor` and uses the OpLog to inform about changes [^also only works when connected to a replicaset].

```java
public interface OplogListener {
    void incomingData(Map<String, Object> data);
}
```

### Profiling Listener

If you need to gather performance data about your mongoDB setup, the Profiling listener has you covered. It gives detailed information about the duration of any write or read access:

```java
public interface ProfilingListener {
    void readAccess(Query query, long time, ReadAccessType t);

    void writeAccess(Class type, Object o, long time, boolean isNew, WriteAccessType t);
}
```

## The Aggregation Framework

The aggregation framework is a very powerful feature of MongoDB and _Morphium_ supports it from the start[^does not work with the `InMemoryDriver' yet]. But with _Morphium_ V4.2.x we made use of it a lot easier. Core of the aggregation Framework in _Morphium_ is the `Aggregator`. This will be created (using the configured `AggregatorFactory`) by a `Morphium` instance.

```java
Aggregator<Source,Result> aggregator=morphium.createAggregator(Source.class,Result.class);
```

This creates an aggregator that reads from the entity `Source` (or better the corresponding collection) and returns the results in `Result`. Usually you will have to define a `Result` entity in order to use aggregation, but with _Morphium_ V4.2 it is possible to have a `Map` as a result class.
After preparing the aggregator, you need to define the stages. All currently available stages are also available in _Morphium._ For a list of available stages, just consult the [mongodb documentation](https://docs.mongodb.com/manual/core/aggregation-pipeline/).

In a nutshell, the aggregation framework runs all documents through a pipeline of commands, that either reduce the input (like a query), change the output (a projection) or calculate some values (like with sum count etc).
The most important pipeline stage is probably the "group" stage. This is similar to the `group by` in SQL, but more powerful, as you can have several of those `group stages` in a pipeline.

here an Example with a simple pipeline:

```java
Aggregator<UncachedObject, Aggregate> a = morphium.createAggregator(UncachedObject.class, Aggregate.class);
assertNotNull(a.getResultType());;
//reduce input
a = a.project("counter");
//Filter
a = a.match(morphium.createQueryFor(UncachedObject.class)
      .f("counter").gt(100));
//Sort, used with $first/$last
a = a.sort("counter");
//limit data
a = a.limit(15);
//group by - here we only have one static group, but could be any field or value
a = a.group("all").avg("schnitt", "$counter").sum("summe", "$counter").sum("anz", 1).last("letzter", "$counter").first("erster", "$counter").end();

//result projection
HashMap<String, Object> projection = new HashMap<>();
projection.put("summe", 1);
projection.put("anzahl", "$anz");
projection.put("schnitt", 1);
projection.put("last", "$letzter");
projection.put("first", "$erster");
a = a.project(projection);

List<Aggregate> lst = a.aggregate();
assert (lst.size() == 1) : "Size wrong: " + lst.size();
log.info("Sum  : " + lst.get(0).getSumme());
log.info("Avg  : " + lst.get(0).getSchnitt());
log.info("Last :    " + lst.get(0).getLast());
log.info("First:   " + lst.get(0).getFirst());
log.info("count:  " + lst.get(0).getAnzahl());


assert (lst.get(0).getAnzahl() == 15) : "did not find 15, instead found: " + lst.get(0).getAnzahl();
```

But you could have that result grouped again for example or add fields to it or change values or ....

Consult the MongoDB documentation for more information about the aggregation pipeline.

### Aggregation Expressions

MongoDB has support for an own expression language, that is mainly used in aggregation. \_Morphium_s representation thereof is `Expr`.
`Expr` does have a lot of factory methods to create special `Expr` instances, for example `Expr.string()` returns a string expression (string constant), `Expr.gt()` creates the "greater than" expression and so on.
Examples of expressions:

```java
Expr e = Expr.add(Expr.field("the_field"), Expr.abs(Expr.field("test")), Expr.doubleExpr(128.0));
Object o = e.toQueryObject();
String val = Utils.toJsonString(o);
log.info(val);
assert(val.equals("{ \"$add\" :  [ \"$the_field\", { \"$abs\" :  [ \"$test\"] } , 128.0] } "));

e = Expr.in(Expr.doubleExpr(1.2), Expr.arrayExpr(Expr.intExpr(12), Expr.doubleExpr(1.2), Expr.field("testfield")));
val=Utils.toJsonString(e.toQueryObject());
log.info(val);
assert(val.equals("{ \"$in\" :  [ 1.2,  [ 12, 1.2, \"$testfield\"]] } "));

e = Expr.zip(Arrays.asList(Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14)), Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14))), Expr.bool(true), Expr.field("test"));
val=Utils.toJsonString(e.toQueryObject());
log.info(val);
assert(val.equals("{ \"$zip\" : { \"inputs\" :  [  [ 1, 14],  [ 1, 14]], \"useLongestLength\" : true, \"defaults\" : \"$test\" }  } "));

e = Expr.filter(Expr.arrayExpr(Expr.intExpr(1), Expr.intExpr(14), Expr.string("asV")), "str", Expr.string("NEN"));
val=Utils.toJsonString(e.toQueryObject());
log.info(val);
assert(val.equals("{ \"$filter\" : { \"input\" :  [ 1, 14, \"asV\"], \"as\" : \"str\", \"cond\" : \"NEN\" }  } "));
```

the output of this little program would be:

```java
{ "$add" :  [ "$the_field", { "$abs" :  [ "$test"] } , 128.0] }
{ "$in" :  [ 1.2,  [ 12, 1.2, "$testfield"]] }
{ "$zip" : { "inputs" :  [  [ 1, 14],  [ 1, 14]], "useLongestLength" : true, "defaults" : "$test" }  }
{ "$filter" : { "input" :  [ 1, 14, "asV"], "as" : "str", "cond" : "NEN" }  }
```

This way you can create complex aggregation pipelines:

```java
	 Aggregator<UncachedObject, Aggregate> a = morphium.createAggregator(UncachedObject.class, Aggregate.class);
	        assertNotNull(a.getResultType());;
	        a = a.project(UtilsMap.of("counter", (Object) Expr.intExpr(1)).add("cnt2", Expr.field("counter")));
	        a = a.match(Expr.gt(Expr.field("counter"), Expr.intExpr(100)));
	        a = a.sort("counter");
	        a = a.limit(15);
	        a = a.group(Expr.string(null)).expr("schnitt", Expr.avg(Expr.field("counter"))).expr("summe", Expr.sum(Expr.field("counter"))).expr("anz", Expr.sum(Expr.intExpr(1))).expr("letzter", Expr.last(Expr.field("counter"))).expr("erster", Expr.first(Expr.field("counter"))).end();
```

This expression language can also be used in queries:

```java
			Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.expr(Expr.gt(Expr.field(UncachedObject.Fields.counter), Expr.intExpr(50)));
        log.info(Utils.toJsonString(q.toQueryObject()));
        List<UncachedObject> lst = q.asList();
        assert (lst.size() == 50) : "Size wrong: " + lst.size();


        for (UncachedObject u : q.q().asList()) {
            u.setDval(Math.random() * 100);
            morphium.store(u);
        }

        q = q.q().expr(Expr.gt(Expr.field(UncachedObject.Fields.counter), Expr.field(UncachedObject.Fields.dval)));
        lst = q.asList();
```

Hint: if you use `Expr` in your code, it is probably a good idea to use `import static de.caluga.morphium.aggregation.Expr.*;` to make the code easier to read and understand.

## Additional information sources

There are some places, you also might want to look at for additional information on mongodb or _Morphium_:

- The mongodb [manual](https://docs.mongodb.com/manual/introduction/), especially the part about [aggregation pipelines](https://docs.mongodb.com/manual/core/aggregation-pipeline/#pipeline)
- The [caluga blog](https://caluga.de), there are some articles on how to use _Morphium_ and related projects and examples. Also, this document itself is available [there](https://boesebeck.name/v/2014/9/5/morphium_documentation?lang=en).[^this blog is powered by _Morphium_ and mongodb]

## Code Examples

### Cache Synchronization

```java
 Messaging msg = new Messaging(morphium, 100, true);
        msg.start();
        MessagingCacheSynchronizer cs = new MessagingCacheSynchronizer(msg, morphium);

        Query<Msg> q = morphium.createQueryFor(Msg.class);
        long cnt = q.countAll();
        assert (cnt == 0) : "Already a message?!?! " + cnt;

        cs.sendClearMessage(CachedObject.class, "test");
        Thread.sleep(2000);
        TestUtils.waitForWrites(morphium,log);
        cnt = q.countAll();
        assert (cnt == 1) : "there should be one msg, there are " + cnt;
        msg.terminate();
        cs.detach();
```

### Geo Spacial Search

```java
    @Test
    public void nearTest() throws Exception {
        morphium.dropCollection(Place.class);
        ArrayList<Place> toStore = new ArrayList<Place>();
    //        morphium.ensureIndicesFor(Place.class);
        for (int i = 0; i < 1000; i++) {
            Place p = new Place();
            List<Double> pos = new ArrayList<Double>();
            pos.add((Math.random() * 180) - 90);
            pos.add((Math.random() * 180) - 90);
            p.setName("P" + i);
            p.setPosition(pos);
            toStore.add(p);
        }
        morphium.storeList(toStore);

        Query<Place> q = morphium.createQueryFor(Place.class).f("position").near(0, 0, 10);
        long cnt = q.countAll();
        log.info("Found " + cnt + " places around 0,0 (10)");
        List<Place> lst = q.asList();
        for (Place p : lst) {
            log.info("Position: " + p.getPosition().get(0) + " / " + p.getPosition().get(1));
        }
    }

    @Index("position:2d")
    @NoCache
    @WriteBuffer(false)
    @WriteSafety(level = SafetyLevel.MAJORITY)
    @DefaultReadPreference(ReadPreferenceLevel.PRIMARY)
    @Entity
    public static class Place {
        @Id
        private ObjectId id;

        public List<Double> position;
        public String name;

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
            this.id = id;
        }

        public List<Double> getPosition() {
            return position;
        }

        public void setPosition(List<Double> position) {
            this.position = position;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
```

### Iterator

```java
    @Test
    public void basicIteratorTest() throws Exception {
        createUncachedObjects(1000);

        Query<UncachedObject> qu = getUncachedObjectQuery();
        long start = System.currentTimeMillis();
        MorphiumIterator<UncachedObject> it = qu.asIterable(2);
        assert (it.hasNext());
        UncachedObject u = it.next();
        assert (u.getCounter() == 1);
        log.info("Got one: " + u.getCounter() + "  / " + u.getValue());
        log.info("Current Buffersize: " + it.getCurrentBufferSize());
        assert (it.getCurrentBufferSize() == 2);

        u = it.next();
        assert (u.getCounter() == 2);
        u = it.next();
        assert (u.getCounter() == 3);
        assert (it.getCount() == 1000);
        assert (it.getCursor() == 3);

        u = it.next();
        assert (u.getCounter() == 4);
        u = it.next();
        assert (u.getCounter() == 5);

        while (it.hasNext()) {
            u = it.next();
            log.info("Object: " + u.getCounter());
        }

        assert (u.getCounter() == 1000);
        log.info("Took " + (System.currentTimeMillis() - start) + " ms");
    }
```

### Asynchronous Read

```java
      @Test
    public void asyncReadTest() throws Exception {
        asyncCall = false;
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("counter").lt(1000);
        q.asList(new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                log.info("got read answer");
                assertNotNull(result,"Error");
                assert (result.size() == 100) : "Error";
                asyncCall = true;
            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                assert false;
            }
        });
        waitForAsyncOperationToStart(1000000);
        int count = 0;
        while (q.getNumberOfPendingRequests() > 0) {
            count++;
            assert (count < 10);
            System.out.println("Still waiting...");
            Thread.sleep(1000);
        }
        assert (asyncCall);
    }
```

### Asynchronous Write

```java
	@Test
	public void asyncStoreTest() throws Exception {
	  asyncCall = false;
	  super.createCachedObjects(1000);
	  TestUtils.waitForWrites(morphium,log);
	  log.info("Uncached object preparation");
	  super.createUncachedObjects(1000);
	  TestUtils.waitForWrites(morphium,log);
	  Query<UncachedObject> uc = morphium.createQueryFor(UncachedObject.class);
	  uc = uc.f("counter").lt(100);
	  morphium.delete(uc, new AsyncOperationCallback<Query<UncachedObject>>() {
	      @Override
	      public void onOperationSucceeded(AsyncOperationType type, Query<Query<UncachedObject>> q, long duration, List<Query<UncachedObject>> result, Query<UncachedObject> entity, Object... param) {
	                log.info("Objects deleted");
	      }

      @Override
      public void onOperationError(AsyncOperationType type, Query<Query<UncachedObject>> q, long duration, String error, Throwable t, Query<UncachedObject> entity, Object... param) {
                assert false;
            }
     });

    uc = uc.q();
    uc.f("counter").mod(3, 2);
        morphium.set(uc, "counter", 0, false, true, new AsyncOperationCallback<UncachedObject>() {
            @Override
            public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                log.info("Objects updated");
                asyncCall = true;

            }

            @Override
            public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                log.info("Objects update error");
            }
    });

    TestUtils.waitForWrites(morphium,log);

    assert(morphium.createQueryFor(UncachedObject.class).f("counter").eq(0).countAll() > 0);
    assert (asyncCall);
}
```

## Disclaimer

This document was written by the authors with most care, but there is no guarantee for 100% accuracy. If you have any questions, find a mistake or have suggestions for improvements, please contact the authors of this document and the developers of morphium via [github.com/sboesebeck/morphium](https://www.github.com/sboesebeck/morphium) or send an email to [sb@caluga.de](mailto:sb@caluga.de)
