package de.caluga.test.morphium.driver;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class InMemDriverTest extends MorphiumInMemTestBase {
    private Logger log = LoggerFactory.getLogger(InMemDriverTest.class);
    private String db = "testing";
    private String coll = "testcoll";

    @Test
    public void inMemTest() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        HelloCommand cmd = new HelloCommand(drv);
        var hello = cmd.execute();

        log.info(hello.toString());
        CreateIndexesCommand cimd = new CreateIndexesCommand(drv).addIndex(new IndexDescription().setKey(Doc.of("counter", 1)));
        cimd.setDb(db).setColl("testcoll1");
        cimd.execute();
//        drv.createIndex(db,"testcoll1", Doc.of("counter",1),Doc.of());

        ListIndexesCommand lcmd = new ListIndexesCommand(drv).setDb(db).setColl("testcoll1");
        var ret = lcmd.execute();
        log.info("Indexes: " + ret.size());
        assertEquals(ret.size(), 2);

        boolean exc = false;
        try {
            drv.createIndex(db, "testcoll1", Doc.of("counter", 1), Doc.of("name", "dings", "unique", true));
        } catch (MorphiumDriverException e) {
            exc = true;
        }
        assertFalse(exc, "Creating the same index does not throw an Exception anymore");

        ShutdownCommand shutdownCommand = new ShutdownCommand(drv).setTimeoutSecs(10);
        var sh = shutdownCommand.execute();
        log.info("Result: " + Utils.toJsonString(sh));
        assertNotSame(sh.get("ok"), 1.0);

        var stepDown = new StepDownCommand(drv).setTimeToStepDown(10);
        sh = stepDown.execute();
        log.info("Result: " + Utils.toJsonString(sh));
        assertNotSame(sh.get("ok"), 1.0);


        InsertMongoCommand insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 123, "strVal", "Hello World")));
        var insertResult = insert.execute();
        log.info("Result: " + Utils.toJsonString(insertResult));
        assertEquals(insertResult.get("n"), 1);
        new InsertMongoCommand(drv).setColl(coll).setDb(db);
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13, "strVal", "Hello"),
                Doc.of("_id", new MorphiumId(), "value", 14, "strVal", "Hello2"),
                Doc.of("_id", new MorphiumId(), "value", 15, "strVal", "Hello3")
        ));
        insertResult = insert.execute();
        assertEquals(insertResult.get("n"), 3);

        FindCommand fnd = new FindCommand(drv).setColl(coll).setDb(db);
        fnd.setFilter(Doc.of("value", 14));
        var found = fnd.execute();
        assertEquals(1, found.size());

        fnd.setFilter(null);
        found = fnd.execute();
        assertEquals(4, found.size());

        DistinctMongoCommand distinct = new DistinctMongoCommand(drv).setKey("strVal");
        distinct.setDb(db).setColl(coll);
        var distinctResult = distinct.execute();
        log.info("Distinct values: " + distinctResult.size());
        assertEquals(4, distinctResult.size());

        CreateIndexesCommand createIndexesCommand = new CreateIndexesCommand(drv).setDb(db).setColl(coll);
        createIndexesCommand.addIndex(new IndexDescription().setKey(Doc.of("strVal", 1)));
        createIndexesCommand.execute();

        CollStatsCommand collStatsCommand = new CollStatsCommand((MorphiumDriver) drv).setDb(db).setColl(coll);
        var collStats = collStatsCommand.execute();
        assertNotNull(collStats.get("nindexes"));
        assertTrue((int) collStats.get("nindexes") > 1);
        assertTrue((long) collStats.get("totalSize") > 0);
        CountMongoCommand count = new CountMongoCommand(drv).setColl(coll).setDb(db).setQuery(Doc.of());
        assertEquals(4, count.getCount());
        ClearCollectionCommand clr = new ClearCollectionCommand(drv).setColl(coll).setDb(db);
        var cleared = clr.execute();
        assertEquals(drv.getDatabase(db).get(coll).size(), 0);
        drv.close();
    }

    @Test
    public void testQuery() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        var insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13, "strVal", "Hello"),
                Doc.of("_id", new MorphiumId(), "value", "Hello2", "strVal", "Hello2"),
                Doc.of("_id", new MorphiumId(), "value", 15, "strVal", "Hello3")
        ));
        var insertResult = insert.execute();

        Query<Map> q = new Query<>(null, Map.class, null);
        q.expr(Expr.eq(Expr.field("value"), Expr.field("strVal")));
        log.info("Query:" + Utils.toJsonString(q.toQueryObject()));
        var ret = drv.find(db, coll, q.toQueryObject(), null, null, 0, 0);
        log.info("Got result: " + ret.size());
        assertTrue(ret.size() == 1);
        assertEquals(ret.get(0).get("strVal"), ret.get(0).get("value"));
        drv.close();
    }

    @Test
    public void testExpire() throws Exception {
        InMemoryDriver drv = new InMemoryDriver().setExpireCheck(10000);
        drv.connect();

        var insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13, "timestamp", new Date()),
                Doc.of("_id", new MorphiumId(), "value", "Hello2", "timestamp", new Date()),
                Doc.of("_id", new MorphiumId(), "value", 15, "timestamp", new Date())
        ));
        var insertResult = insert.execute();
        CreateIndexesCommand indexesCommand = new CreateIndexesCommand(drv)
                .setDb(db).setColl(coll)
                .addIndex(new IndexDescription().setKey(Doc.of("timestamp", 1)).setExpireAfterSeconds(8));
        indexesCommand.execute();
        Thread.sleep(1500);
        var idx = drv.getIndexes(db, coll);
        while (drv.find(db, coll, Doc.of(), null, null, 0, 0).size() > 0) {
            log.info("Waiting for elements to be removed: " + drv.find(db, coll, Doc.of(), null, null, 0, 0).size());
            Thread.sleep(1000);
        }
        drv.close();
    }

    @Test
    public void testUniqueIndex() throws Exception {
        assertThrows(MorphiumDriverException.class, () -> {
            InMemoryDriver drv = new InMemoryDriver().setExpireCheck(10000);
            drv.connect();
            try (drv) {

                var insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
                insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13),
                        Doc.of("_id", new MorphiumId(), "value", 14),
                        Doc.of("_id", new MorphiumId(), "value", 15)
                ));
                var insertResult = insert.execute();
                CreateIndexesCommand indexesCommand = new CreateIndexesCommand(drv)
                        .setDb(db).setColl(coll)
                        .addIndex(new IndexDescription().setKey(Doc.of("value", 1)).setUnique(true));
                indexesCommand.execute();
                insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
                insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13),
                        Doc.of("_id", new MorphiumId(), "value", 17),
                        Doc.of("_id", new MorphiumId(), "value", 19)
                ));
                insert.execute();
            }
        });
    }


    @Test
    public void testUpdate() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        var insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13, "strVal", "Hello"),
                Doc.of("_id", new MorphiumId(), "value", "Hello2", "strVal", "Hello2"),
                Doc.of("_id", new MorphiumId(), "value", 15, "strVal", "Hello3")
        ));
        var insertResult = insert.execute();

        UpdateMongoCommand update = new UpdateMongoCommand(drv).setDb(db).setColl(coll);
        update.addUpdate(Doc.of("value", "Hello2"), Doc.of("$set", Doc.of("strVal", "New Value")), null, false, false, null, null, null);
        var updateResult = update.execute();
        Query<Map> q = new Query<>(null, Map.class, null);
        q.expr(Expr.eq(Expr.field("value"), Expr.field("strVal")));
        log.info("Query:" + Utils.toJsonString(q.toQueryObject()));
        var ret = drv.find(db, coll, q.toQueryObject(), null, null, 0, 0);
        log.info("Got result: " + ret.size());
        assertTrue(ret.size() == 0);

        drv.close();
    }

    @Test
    public void testUpdate_pullAll() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        var insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
        String id = "12345";
        //   db.testcoll.insertOne({_id: "12345", scores: [0,2,5,5,1,0]})
        insert.setDocuments(Arrays.asList(Doc.of("_id", id, "scores", Arrays.asList(0,2,5,5,1,0))));
        var insertResult = insert.execute();

        UpdateMongoCommand update = new UpdateMongoCommand(drv).setDb(db).setColl(coll);
        //   db.testcoll.updateMany({_id: "12345"}, {$pullAll: {"scores": [0,5]}} )
        update.addUpdate(Doc.of("_id", id), Doc.of("$pullAll", Doc.of("scores", Arrays.asList(0,5))), null, false, false, null, null, null);
        log.info("Update:" + Utils.toJsonString(update.asMap().toString()));
        var updateResult = update.execute();

        //   db.testcoll.find({_id: "12345"})
        List<Map<String, Object>> ret = drv.find(db, coll, Doc.of("_id", id), null, null, 0, 0);
        log.info("Got result: " + ret.size());
        List scores = (List) ret.get(0).get("scores");
        assertTrue(ret.size() == 1);
        assertTrue(scores.get(0) == Integer.valueOf(2));
        assertTrue(scores.get(1) == Integer.valueOf(1));

        drv.close();
    }


    @Test
    public void testUpdate_pull_remove_all_items_that_equal_to_a_specified_value() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        var insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
        String id1 = "1", id2 = "2";
        // db.testcoll.insertMany( [{_id: 1, fruits: [ "apples", "pears", "oranges", "grapes", "bananas" ],
        //        vegetables: [ "carrots", "celery", "squash", "carrots" ]},
        //    {_id: 2, fruits: [ "plums", "kiwis", "oranges", "bananas", "apples" ],
        //       vegetables: [ "broccoli", "zucchini", "carrots", "onions" ]}] )
        insert.setDocuments(Arrays.asList(Doc.of("_id", id1, "fruits", Arrays.asList("apples", "pears", "oranges", "grapes", "bananas"),
                        "vegetables", Arrays.asList("carrots", "celery", "squash", "carrots")),
                Doc.of("_id", id2, "fruits", Arrays.asList("plums", "kiwis", "oranges", "bananas", "apples"),
                        "vegetables", Arrays.asList("broccoli", "zucchini", "carrots", "onions"))));
        var insertResult = insert.execute();

        UpdateMongoCommand update = new UpdateMongoCommand(drv).setDb(db).setColl(coll);
        //   db.testcoll.updateMany( { }, { $pull: { fruits: { $in: [ "apples", "oranges" ] }, vegetables: "carrots" } })
        update.addUpdate(Doc.of(), Doc.of("$pull", Doc.of("fruits", Doc.of("$in", Arrays.asList("apples", "oranges")),
                "vegetables", "carrots")), null, false, true, null, null, null);
        log.info("Update:" + Utils.toJsonString(update.asMap().toString()));
        var updateResult = update.execute();

        //   db.testcoll.find({_id: "1"})
        //       { _id: 1, fruits: [ "pears", "grapes", "bananas" ], vegetables: [ "celery", "squash" ] },
        List<Map<String, Object>> ret1 = drv.find(db, coll, Doc.of("_id", id1), null, null, 0, 0);
        List fruits1 = (List) ret1.get(0).get("fruits");
        List vegetables1 = (List) ret1.get(0).get("vegetables");
        assertTrue(fruits1.equals(List.of("pears", "grapes", "bananas")));
        assertTrue(vegetables1.equals(List.of("celery", "squash")));

        //   db.testcoll.find({_id: "2"})
        //      { _id: 2, fruits: [ "plums", "kiwis", "bananas" ], vegetables: [ "broccoli", "zucchini", "onions" ] }
        List<Map<String, Object>> ret2 = drv.find(db, coll, Doc.of("_id", id2), null, null, 0, 0);
        List fruits2 = (List) ret2.get(0).get("fruits");
        List vegetables2 = (List) ret2.get(0).get("vegetables");
        assertTrue(fruits2.equals(List.of("plums", "kiwis", "bananas")));
        assertTrue(vegetables2.equals(List.of("broccoli", "zucchini", "onions")));

        drv.close();
    }

    @Test
    public void testUpdate_pull_remove_all_items_that_match_a_specified_condition() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        var insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
        String id1 = "1";
        // db.testcoll.insertOne( { _id: 1, votes: [ 3, 5, 6, 7, 7, 8 ] } )
        insert.setDocuments(Arrays.asList(Doc.of("_id", id1, "votes", Arrays.asList(3, 5, 6, 7, 7, 8))));
        var insertResult = insert.execute();

        UpdateMongoCommand update = new UpdateMongoCommand(drv).setDb(db).setColl(coll);
        //   db.testcoll.updateOne( { _id: 1 }, { $pull: { votes: { $gte: 6 } } } )
        update.addUpdate(Doc.of(), Doc.of("$pull", Doc.of("votes", Doc.of("$gte", 6))), null, false, false, null, null, null);
        log.info("Update:" + Utils.toJsonString(update.asMap().toString()));
        var updateResult = update.execute();

        //   db.testcoll.find({_id: "1"})
        //       { _id: 1, votes: [  3,  5 ] },
        List<Map<String, Object>> ret1 = drv.find(db, coll, Doc.of("_id", id1), null, null, 0, 0);
        List votes = (List) ret1.get(0).get("votes");
        assertTrue(votes.equals(List.of(3,  5)));

        drv.close();
    }

    @Test
    public void testCapped() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        var create = new CreateCommand(drv).setColl(coll).setDb(db).setCapped(true).setMax(5).setSize(100000);
        create.execute();

        var insert = new InsertMongoCommand(drv).setColl(coll).setDb(db);
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13, "strVal", "Hello"),
                Doc.of("_id", new MorphiumId(), "value", "Hello2", "strVal", "Hello2"),
                Doc.of("_id", new MorphiumId(), "value", 15, "strVal", "Hello3"),
                Doc.of("_id", new MorphiumId(), "value", 132, "strVal", "Hello3"),
                Doc.of("_id", new MorphiumId(), "value", 1, "strVal", "Hello3")
        ));
        var insertResult = insert.execute();
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13, "strVal", "Hello"),
                Doc.of("_id", new MorphiumId(), "value", "Hello2", "strVal", "Hello2"),
                Doc.of("_id", new MorphiumId(), "value", 15, "strVal", "Hello3"),
                Doc.of("_id", new MorphiumId(), "value", 132, "strVal", "Hello3"),
                Doc.of("_id", new MorphiumId(), "value", 1, "strVal", "Hello3")
        ));
        insertResult = insert.execute();
        var ret = drv.find(db, coll, Doc.of(), null, null, 0, 0);
        log.info("Got result: " + ret.size());
        assertEquals(5, ret.size());
        insert.setDocuments(Arrays.asList(Doc.of("_id", new MorphiumId(), "value", 13, "strVal", "Hello"),
                Doc.of("_id", new MorphiumId(), "value", "Hello2", "strVal", "Hello2"),
                Doc.of("_id", new MorphiumId(), "value", 15, "strVal", "Hello3"),
                Doc.of("_id", new MorphiumId(), "value", 132, "strVal", "Hello3"),
                Doc.of("_id", new MorphiumId(), "value", 1, "strVal", "Hello3")
        ));
        insertResult = insert.execute();
        ret = drv.find(db, coll, Doc.of(), null, null, 0, 0);
        assertTrue(ret.size() == 5);
        log.info("Got result: " + ret.size());
        drv.close();
    }
}
