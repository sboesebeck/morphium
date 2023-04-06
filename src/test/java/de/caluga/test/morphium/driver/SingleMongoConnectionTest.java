package de.caluga.test.morphium.driver;

import de.caluga.morphium.Utils;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.commands.auth.SaslAuthCommand;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.net.Socket;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class SingleMongoConnectionTest extends ConnectionTestBase {

    private Logger log = LoggerFactory.getLogger(SingleMongoConnectionTest.class);

    //    @Test(expected = MorphiumDriverException.class)
    //    public void failTest() throws Exception{
    //        try (var con = getConnection()) {
    //
    //            ShutdownCommand cmd = new ShutdownCommand(con);
    //            cmd.setForce(true);
    //            cmd.setTimeoutSecs(2);
    //            var msg = con.sendCommand(cmd.asMap());
    //            log.info("Sent Shutdown command...");
    //            Thread.sleep(3000);
    //            //server should be down now
    //            log.info("Should be down now. Sending hello...");
    //
    //            HelloCommand c = new HelloCommand(con).setHelloOk(true);
    //            int sent = 0;
    //            while (true) {
    //                log.info("Sending message...");
    //                assertThat(sent).isLessThan(3);
    //                msg = con.sendCommand(c.asMap());
    //                sent++;
    //                Thread.sleep(500);
    //            }
    //
    //        }
    //    }

    @Test
    public void testSyncConnection() throws Exception {
        var con = getConnection();
        log.info("Connected");
        int deleted = (int) new ClearCollectionCommand(con).setColl(coll).setDb(db).doClear();
        log.info("Deleted old data: " + deleted);
        ObjectMapperImpl objectMapper = new ObjectMapperImpl();

        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            StoreMongoCommand cmd = new StoreMongoCommand(con).setDb(db).setColl(coll).setDocuments(Arrays.asList(Doc.of(objectMapper.serialize(o))));
            cmd.execute();
        }

        log.info("created test data");
        log.info("running find...");
        FindCommand fnd = new FindCommand(con).setColl(coll).setDb(db).setBatchSize(10).setFilter(Doc.of("counter", 123));
        List<Map<String, Object>> res = fnd.execute();
        assertEquals(1, res.size());
        assertEquals(123, res.get(0).get("counter"));
        log.info("done.");
        log.info("Updating...");
        UpdateMongoCommand update = new UpdateMongoCommand(con);
        update.addUpdate(Doc.of("q", Doc.of("_id", res.get(0).get("_id")), "u", Doc.of("$set", Doc.of("counter", 9999))));
        update.setOrdered(false);
        update.setDb(db);
        update.setColl(coll);
        Map<String, Object> updateInfo = update.execute();
        assertEquals(1, updateInfo.get("nModified"));
        log.info("...done");
        log.info("Re-Reading...");
        fnd.setConnection(con);
        fnd.setFilter(Doc.of("_id", res.get(0).get("_id")));
        res = fnd.execute();
        assertEquals(1, res.size());
        assertEquals(9999, res.get(0).get("counter"));
        con.close();

        for (var e : con.getStats().entrySet()) {
            log.info("Stats: " + e.getKey() + " -> " + e.getValue());
        }
    }

    @Test
    public void testMongoTypes() throws Exception {
        var con = getConnection();
        ObjectMapperImpl mapper = new ObjectMapperImpl();
        mapper.setAnnotationHelper(new AnnotationAndReflectionHelper(true));
        MTestClass cls = new MTestClass();
        cls.stringValue = "strValue";
        cls.charValue = new Character('c');
        cls.longValue = new Long(42);
        cls.integerValue = new Integer(52);
        cls.floatValue = new Float(3.3);
        cls.doubleValue = new Double(34.2d);
        cls.dateValue = new Date();
        cls.booleanValue = new Boolean(true);
        cls.byteValue = new Byte((byte) 0x33);
        cls.shortValue = new Short((short) 12);
        cls.atomicLongValue = new AtomicLong(42);
        cls.atomicBooleanValue = new AtomicBoolean(true);
        cls.atomicIntegerValue = new AtomicInteger(42);
        cls.patternValue = Pattern.compile("a*");
        cls.bigDecimalValue = new BigDecimal(123);
        cls.uuidValue = UUID.randomUUID();
        cls.instantValue = Instant.now();
        cls.localDateValue = LocalDate.now();
        cls.localTimeValue = LocalTime.now();
        cls.localDateTimeValue = LocalDateTime.now();
        InsertMongoCommand cmd = new InsertMongoCommand(con);
        cmd.setDb(db).setColl(coll).setDocuments(Arrays.asList(mapper.serialize(cls)));
        cmd.execute();
        FindCommand fnd = new FindCommand(con);
        fnd.setDb(db).setColl(coll);
        fnd.setFilter(Doc.of());
        fnd.setMaxTimeMS(999999);
        OpMsg q = new OpMsg();
        q.setMessageId(1234);
        q.setFirstDoc(Doc.of(fnd.asMap()));
        var r = con.sendAndWaitForReply(q);
        var lst = fnd.execute();
        Thread.sleep(100);
        var m = lst.get(0);
        var cls2 = mapper.deserialize(MTestClass.class, m);;
        assertEquals(cls.stringValue, cls2.stringValue);
        assertEquals(cls.charValue, cls2.charValue);
        assertEquals(cls.integerValue, cls2.integerValue);
        assertEquals(cls.floatValue, cls2.floatValue);
        assertEquals(cls.doubleValue, cls2.doubleValue);
        assertEquals(cls.dateValue, cls2.dateValue);
        assertEquals(cls.booleanValue, cls2.booleanValue);
        assertEquals(cls.shortValue, cls2.shortValue);
        assertEquals(cls.byteValue, cls2.byteValue);
        assertEquals(cls.atomicLongValue.get(), cls2.atomicLongValue.get());
        assertEquals(cls.atomicBooleanValue.get(), cls2.atomicBooleanValue.get());
        assertEquals(cls.atomicIntegerValue.get(), cls2.atomicIntegerValue.get());
        assertTrue(cls.patternValue.toString().equals(cls2.patternValue.toString()));
        assertEquals(cls.bigDecimalValue, cls2.bigDecimalValue);
        assertEquals(cls.uuidValue, cls2.uuidValue);
        assertEquals(cls.instantValue, cls2.instantValue);
        assertEquals(cls.localDateValue, cls2.localDateValue);
        assertEquals(cls.localDateTimeValue, cls2.localDateTimeValue);
        assertEquals(cls.localTimeValue, cls2.localTimeValue);
        con.close();
    }

    @Entity()
    public static class MTestClass {
        @Id
        public MorphiumId id;
        public String stringValue;
        public Character charValue;
        public Integer integerValue;
        public Long longValue;
        public Float floatValue;
        public Double doubleValue;
        public Date dateValue;
        public Boolean booleanValue;
        public Byte byteValue;
        public Short shortValue;
        public AtomicBoolean atomicBooleanValue;
        public AtomicInteger atomicIntegerValue;
        public AtomicLong atomicLongValue;
        public Pattern patternValue;
        public BigDecimal bigDecimalValue;
        public UUID uuidValue;
        public Instant instantValue;
        public LocalDate localDateValue;
        public LocalTime localTimeValue;
        public LocalDateTime localDateTimeValue;

        public void checkNotNull() {
            assertNotNull(stringValue);
            assertNotNull(charValue);
            assertNotNull(integerValue);
            assertNotNull(floatValue);
            assertNotNull(doubleValue);
            assertNotNull(dateValue);
            assertNotNull(booleanValue);
            assertNotNull(shortValue);
            assertNotNull(byteValue);
            assertNotNull(atomicLongValue);
            assertNotNull(atomicBooleanValue);
            assertNotNull(atomicIntegerValue);
            assertNotNull(patternValue);
            assertNotNull(bigDecimalValue);
            assertNotNull(uuidValue);
            assertNotNull(instantValue);
            assertNotNull(localDateValue);
            assertNotNull(localDateTimeValue);
            assertNotNull(localTimeValue);
        }
    }

    @Test
    public void testUpdateSyncConnection() throws Exception {
        var con = getConnection();
        new ClearCollectionCommand(con).setDb(db).setColl(coll).execute();
        //log.info("Deleted old data: " + deleted);
        ObjectMapperImpl objectMapper = new ObjectMapperImpl();

        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            StoreMongoCommand cmd = new StoreMongoCommand(con).setDb(db).setColl(coll).setDocuments(Arrays.asList(Doc.of(objectMapper.serialize(o))));
            cmd.execute();
        }

        log.info("created test data");
        log.info("running find...");
        FindCommand fnd = new FindCommand(con).setColl(coll).setDb(db).setBatchSize(100).setFilter(Doc.of("counter", 123));
        List<Map<String, Object>> res = fnd.execute();
        log.info("got result");
        assertEquals(1, res.size());
        assertEquals(123, res.get(0).get("counter"));
        log.info("done.");
        log.info("Updating...");
        UpdateMongoCommand update = new UpdateMongoCommand(con).setDb(db).setColl(coll).addUpdate(Doc.of("q", Doc.of("_id", res.get(0).get("_id")), "u", Doc.of("$set", Doc.of("counter", 9999))));
        Map<String, Object> updateInfo = update.execute();
        assertEquals(1, updateInfo.get("nModified"));
        log.info("...done");
        log.info("Re-Reading...");
        fnd.setFilter(Doc.of("_id", res.get(0).get("_id")));
        fnd.setConnection(con);
        res = fnd.execute();
        assertEquals(1, res.size());
        assertEquals(9999, res.get(0).get("counter"));
        con.close();
    }

    @Test
    public void testWatchDb() throws Exception {
        var con = getConnection();
        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        UncachedObject o = new UncachedObject("value", 123);
        StoreMongoCommand cmd = new StoreMongoCommand(con).setDb(db).setColl(coll).setDocuments(Arrays.asList(Doc.of(objectMapper.serialize(o))));
        cmd.execute();
        long start = System.currentTimeMillis();
        new Thread() {
            public void run() {
                try {
                    log.info("Thread is connecting...");
                    SingleMongoConnectDriver con = new SingleMongoConnectDriver();
                    con.setHostSeed("localhost:27017");
                    con.setCredentials("admin", "test", "test");
                    con.setDefaultBatchSize(5);
                    con.connect();
                    log.info("Thread connected");
                    ObjectMapperImpl objectMapper = new ObjectMapperImpl();
                    log.info("Thread is waiting...");
                    Thread.sleep(3500);
                    UncachedObject o = new UncachedObject("value", 123);
                    UncachedObject o2 = new UncachedObject("value2", 124);
                    StoreMongoCommand cmd =
                        new StoreMongoCommand(con.getConnection()).setDb(db).setColl(coll).setDocuments(Arrays.asList(Doc.of(objectMapper.serialize(o)), Doc.of(objectMapper.serialize(o2))));
                    cmd.execute();
                    cmd.setColl("test2");
                    cmd.execute();
                    log.info("stored data after " + (System.currentTimeMillis() - start) + "ms");
                    con.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } .start();
        con.watch(new WatchCommand(con).setMaxTimeMS(10000).setCb(new DriverTailableIterationCallback() {
            private int counter = 0;
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                log.info("got data after " + (System.currentTimeMillis() - start) + "ms");
                log.info(Utils.toJsonString(data));
                counter++;
            }
            @Override
            public boolean isContinued() {
                return counter < 4;
            }
        }).setBatchSize(1).setColl(null).setDb(db).setMaxTimeMS(1000));
        log.info("Watch ended");
        con.close();
    }

    @Test
    public void testWatchColl() throws Exception {
        var con = getConnection();
        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        UncachedObject o = new UncachedObject("value", 123);
        StoreMongoCommand cmd = new StoreMongoCommand(con).setDb(db).setColl(coll).setDocuments(Arrays.asList(Doc.of(objectMapper.serialize(o))));
        cmd.execute();
        long start = System.currentTimeMillis();
        new Thread() {
            public void run() {
                try {
                    log.info("Thread is connecting...");
                    SingleMongoConnectDriver con = new SingleMongoConnectDriver();
                    con.setHostSeed("localhost:27017");
                    con.setCredentials("admin", "test", "test");
                    con.setDefaultBatchSize(5);
                    con.connect();
                    log.info("Thread connected");
                    ObjectMapperImpl objectMapper = new ObjectMapperImpl();
                    log.info("Thread is waiting...");
                    Thread.sleep(3500);
                    UncachedObject o = new UncachedObject("value", 123);
                    UncachedObject o2 = new UncachedObject("value2", 124);
                    StoreMongoCommand cmd =
                        new StoreMongoCommand(con.getConnection()).setDb(db).setColl(coll).setDocuments(Arrays.asList(Doc.of(objectMapper.serialize(o)), Doc.of(objectMapper.serialize(o2))));
                    cmd.execute();
                    cmd.setColl("test2");
                    cmd.execute();
                    log.info("stored data after " + (System.currentTimeMillis() - start) + "ms");
                    con.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } .start();
        final AtomicInteger counter = new AtomicInteger(0);
        final long waitStart = System.currentTimeMillis();
        con.watch(new WatchCommand(con).setMaxTimeMS(10000).setCb(new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                log.info("got data after " + (System.currentTimeMillis() - start) + "ms");
                log.info(Utils.toJsonString(data));
                counter.incrementAndGet();
            }
            @Override
            public boolean isContinued() {
                return System.currentTimeMillis() - waitStart < 5000;
            }
        }).setBatchSize(1).setColl(coll).setDb(db).setMaxTimeMS(1000));
        log.info("Watch ended");
        assertThat(counter.get()).isLessThanOrEqualTo(2);
        con.close();
    }

    @Test
    public void testMapReduce() throws Exception {
        var con = getConnection();
        Object deleted = new ClearCollectionCommand(con).setDb(db).setColl(coll).execute().get("n");
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();

        for (int i = 0; i < 100; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int)(i * i / (i + 1))))));
        }

        StoreMongoCommand cmd = new StoreMongoCommand(con).setDb(db).setColl(coll).setDocuments(testList);
        cmd.execute();
        MapReduceCommand settings = new MapReduceCommand(con).setColl(coll).setDb(db).setOutConfig(Doc.of("inline", 1)).setMap("function(){ if (this.counter%2==0) emit(this.counter,1); }")
        .setReduce("function(key,values){ return values; }").setFinalize("function(key, reducedValue){ return reducedValue;}").setSort(Doc.of("counter", 1)).setScope(Doc.of("myVar", 1));
        List<Map<String, Object>> res = settings.execute();
        res.sort((o1, o2) -> ((Comparable) o1.get("_id")).compareTo(o2.get("_id")));
        log.info("Got results");
        assertEquals(50, res.size());
        assertTrue(res.get(0).containsKey("_id"));
        assertThat(res.get(0).containsKey("value"));
        assertEquals(1.0, ((List) res.get(14).get("value")).get(0));
        con.close();
    }

    @Test
    public void testRunCommand() throws Exception {
        var con = getConnection();
        Object deleted = new ClearCollectionCommand(con).setDb(db).setColl(coll).execute().get("n");
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();

        for (int i = 0; i < 100; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int)(i * i / (i + 1))))));
        }

        StoreMongoCommand cmd = new StoreMongoCommand(con).setDb(db).setColl(coll).setDocuments(testList);
        cmd.execute();
        var msg = new HelloCommand(con).setHelloOk(true).setIncludeClient(false).executeAsync();
        var result = con.readSingleAnswer(msg);
        assertTrue(result != null);
        assertEquals(result.get("me"), result.get("primary"));
        assertEquals(false, result.get("secondary"));
        con.close();
    }

    @Test
    public void iteratorTest() throws Exception {
        var con = getConnection();
        Object deleted = new ClearCollectionCommand(con).setDb(db).setColl(coll).execute().get("n");
        log.info("Deleted old data: " + deleted);
        List<Map<String, Object>> testList = new ArrayList<>();
        MorphiumObjectMapper om = new ObjectMapperImpl();

        for (int i = 0; i < 1000; i++) {
            testList.add(Doc.of(om.serialize(new UncachedObject("strValue" + i, (int)(i * i / (i + 1))))));
        }

        StoreMongoCommand cmd = new StoreMongoCommand(con).setDb(db).setColl(coll).setDocuments(testList);
        cmd.execute();
        FindCommand fnd = new FindCommand(con).setDb(db).setColl(coll).setBatchSize(17);
        var msg = con.sendCommand(fnd);
        var crs = con.getAnswerFor(msg, 100);
        int cnt = 0;

        while (crs.hasNext()) {
            cnt++;
            var obj = crs.next();
            assertNotNull(obj);
            assertThat(obj).isNotEmpty();
            assertNotNull(obj.get("_id"));
        }

        assertEquals(1000, cnt);
        //GEtAll
        msg = con.sendCommand(fnd);
        crs = con.getAnswerFor(msg, 100);
        List<Map<String, Object>> lst = crs.getAll();
        assertNotNull(lst);
        assertEquals(1000, lst.size());
        assertNotNull(lst.get(0).get("_id"));
        con.close();
    }

    @Test
    public void testHandshake() throws Exception {
        Socket s = new Socket("localhost", 27017);
        var in = s.getInputStream();
        var out = s.getOutputStream();
        OpQuery q = new OpQuery();
        q.setMessageId(100);
        q.setDoc(
            Doc.of("ismaster", true, "helloOk", true, "client", Doc.of("driver", Doc.of("name", "morphium", "version", 1), "os", Doc.of("type", "darvin", "name", "macos", "architecture", "arm64"),
                    "platform", "Java", "version", "0.0.1", "application", Doc.of("name", "Morphium-Test")), "compression", Arrays.asList("none"), "loadBalanced", false));
        q.setColl("admin.$cmd");
        q.setLimit(-1);
        q.setDb(null);
        out.write(q.bytes());
        out.flush();
        var incoming = WireProtocolMessage.parseFromStream(in);
        log.info("incoming: " + incoming.getClass().getSimpleName());
        var reply = (OpReply) incoming;
        log.info(Utils.toJsonString(reply.getDocuments().get(0)));
    }

    @Test
    public void testAuthSHA() throws Exception {
        var con = getConnection();
        InsertMongoCommand insert = new InsertMongoCommand(con);
        insert.setDb("test");
        insert.setDocuments(Arrays.asList(Doc.of("value", "test", "count", 1213)));
        insert.setColl("testcoll");
        insert.setOrdered(true);
        var result = insert.execute();
        log.info("InsertResult: " + Utils.toJsonString(result));
        con.close();
    }

    @Test
    public void testmanualAuthSHA1() throws Exception {
        var con = getConnection();
        //        ScramClient scramClient = ScramClient
        //                .channelBinding(ScramClient.ChannelBinding.NO)
        //                .stringPreparation(NO_PREPARATION)
        //                .selectClientMechanism(ScramMechanisms.SCRAM_SHA_1)
        //                .nonceSupplier(() -> "fyko+d2lbbFgONRv9qkxdawL")
        //                .setup();
        //        ScramSession scramSession = scramClient.scramSession("test");
        //        var msg = scramSession.clientFirstMessage();
        //
        //
        //        byte[] data=msg.getBytes(StandardCharsets.UTF_8);
        //        GenericCommand cmd=new GenericCommand(con);
        //        cmd.setCmdData(Doc.of("saslStart",1,"mechanism","SCRAM-SHA-1","payload",data,"options",Doc.of("skipEmptyExchange",true)).add("$db","admin"));
        //        cmd.setCommandName("saslStart");
        //        cmd.setDb("admin");
        //
        //        var reply=con.readSingleAnswer(cmd.executeAsync());
        //        data= (byte[]) reply.get("payload");
        //        int id=(Integer) reply.get("conversationId");
        //        ScramSession.ServerFirstProcessor serverFirstProcessor = scramSession.receiveServerFirstMessage(new String(data));
        //        log.info("Salt: " + serverFirstProcessor.getSalt() + ", i: " + serverFirstProcessor.getIteration());
        //        String passwd="test";
        //        String user="test";
        //        String pwd=user+":mongo:"+passwd;
        //        MessageDigest md = MessageDigest.getInstance("MD5");
        //        md.update(pwd.getBytes(StandardCharsets.UTF_8));
        //        var md5=md.digest();
        //        StringBuilder hex=new StringBuilder();
        //        for (byte b:md5){
        //            hex.append(Utils.getHex(b).toLowerCase());
        //        }
        //        log.info("Hex: "+hex.toString());
        //        ScramSession.ClientFinalProcessor clientFinalProcessor
        //                = serverFirstProcessor.clientFinalProcessor(hex.toString());
        //        String s1 = clientFinalProcessor.clientFinalMessage();
        //        log.info("Sending: "+ s1);
        //
        //        cmd=new GenericCommand(con);
        //        cmd.setCommandName("saslContinue");
        //        cmd.setCmdData(Doc.of("saslContinue",1,"conversationId",id,"payload",s1.getBytes(StandardCharsets.UTF_8)));
        //        cmd.setDb("admin");
        //        reply=con.readSingleAnswer(cmd.executeAsync());
        //
        //        clientFinalProcessor.receiveServerFinalMessage(new String((byte[])reply.get("payload")));
        SaslAuthCommand auth = new SaslAuthCommand(con);
        auth.setMechanism("SCRAM-SHA-1").setUser("test").setDb("admin").setPassword("test");
        auth.execute();
        log.info("Authentication sucessful");
        //try accessing something
        InsertMongoCommand insert = new InsertMongoCommand(con);
        insert.setDb("test");
        insert.setDocuments(Arrays.asList(Doc.of("value", "test", "count", 1213)));
        insert.setColl("testcoll");
        insert.setOrdered(true);
        var reply = insert.execute();
        //
        //        q=new OpMsg().setFirstDoc(insert.asMap());
        //        q.setMessageId(2002);
        //        con.sendQuery(q);
        //reply=con.readSingleAnswer(2002);
        log.info("Insert: " + Utils.toJsonString(reply));
    }

    @Test
    public void speedTests() throws Exception {
        var con = getConnection();
        //try accessing something
        long start = System.currentTimeMillis();
        InsertMongoCommand insert = new InsertMongoCommand(con);
        insert.setDb("test");
        insert.setDocuments(Arrays.asList(Doc.of("value", "test", "count", 1213)));
        insert.setColl("testcoll");
        insert.setOrdered(true);
        var reply = insert.execute();
        long dur = System.currentTimeMillis();
        log.info("Insert took: " + dur);
        start = System.currentTimeMillis();
        HelloCommand h = new HelloCommand(con);
        h.setColl("admin");
        h.setDb("test");
        h.setHelloOk(true);
        h.setIncludeClient(false);
        h.execute();
        dur = System.currentTimeMillis() - start;
        log.info("Hello took: " + dur);
    }
}
