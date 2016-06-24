package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 27.08.12
 * Time: 11:17
 * <p/>
 */
public class WhereTest extends MongoTest {

    @Test
    public void testWhere() throws Exception {
        super.createUncachedObjects(100);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.where("this.count > 0");
        q.setReadPreferenceLevel(ReadPreferenceLevel.NEAREST);
        q.get();

    }

    //This test was only needed for bugfixing of WriteConcern behavior of Mongodb < 2.2.0
    //    @Test
    //    public void wcTest() throws Exception {
    //
    //        MongoOptions o = new MongoOptions();
    //        o.autoConnectRetry = true;
    //        o.fsync = false;
    //        o.socketTimeout = 15000;
    //        o.connectTimeout = 15000;
    //        o.connectionsPerHost = 10;
    //        o.socketKeepAlive = false;
    //        o.threadsAllowedToBlockForConnectionMultiplier = 5;
    //        o.safe = false;
    ////        o.w=3;
    //
    //
    //        List<ServerAddress> adr = new ArrayList<ServerAddress>();
    //        adr.add(new ServerAddress("mongo1", 27017));
    //        adr.add(new ServerAddress("mongo2", 27017));
    //        adr.add(new ServerAddress("mongo3", 27017));
    //
    //        Mongo mongo = new Mongo(adr, o);
    //        mongo.setReadPreference(ReadPreference.PRIMARY);
    //
    //        ReplicaSetStatus s = morphium.getReplicaSetStatus(true);
    //        System.out.println("Active nodes: " + s.getActiveNodes());
    //        BasicDBObject obj = new BasicDBObject();
    //        obj.put("var", "value");
    //        obj.put("tst", 1234);
    //
    //        DB db = mongo.getDB("test");
    //
    //
    //        DBCollection coll = db.getCollection("test");
    //        coll.setReadPreference(ReadPreference.PRIMARY);
    //        WriteConcern w = new WriteConcern(4, 2000, false, false);
    //        coll.save(obj, w);
    //
    //    }


}
