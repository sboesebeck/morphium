package de.caluga.test.mongo.suite;

import com.mongodb.*;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.replicaset.ReplicaSetStatus;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 27.08.12
 * Time: 11:17
 * <p/>
 * TODO: Add documentation here
 */
public class WhereTest extends MongoTest {

    @Test
    public void testWhere() throws Exception {
        super.createUncachedObjects(100);

        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q = q.where("rs.slaveOk(); db.uncached_object.count({count:{$lt:10}}) > 0 && db.uncached_object.find({ _id: this._id }).count()>0");
        q.setReadPreferenceLevel(ReadPreferenceLevel.ALL_NODES);
        q.get();

    }

    @Test
    public void wcTest() throws Exception {

        MongoOptions o = new MongoOptions();
        o.autoConnectRetry = true;
        o.fsync = true;
        o.socketTimeout = 10000;
        o.connectTimeout = 5000;
        o.connectionsPerHost = 10;
        o.socketKeepAlive = true;
        o.threadsAllowedToBlockForConnectionMultiplier = 5;
        o.safe = false;
//        o.w=3;

        List<ServerAddress> adr = new ArrayList<ServerAddress>();
        adr.add(new ServerAddress("localhost", 27017));
        adr.add(new ServerAddress("localhost", 27018));
        adr.add(new ServerAddress("localhost", 27019));

        Mongo mongo = new Mongo(adr, o);
        mongo.setReadPreference(com.mongodb.ReadPreference.SECONDARY);

        ReplicaSetStatus s = MorphiumSingleton.get().getReplicaSetStatus(true);
        System.out.println("Active nodes: " + s.getActiveNodes());
        BasicDBObject obj = new BasicDBObject();
        obj.put("var", "value");
        obj.put("tst", 1234);

        DB db = mongo.getDB("test");


        DBCollection coll = db.getCollection("test");
        coll.setReadPreference(com.mongodb.ReadPreference.SECONDARY);
        WriteConcern w = new WriteConcern(4, 10000, false, true);
        coll.save(obj, w);

    }


}
