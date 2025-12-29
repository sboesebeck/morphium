package de.caluga.test.mongo.suite.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

@Tag("external")
public class ShardingTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void shardingReplacementTest(Morphium morphium) throws Exception  {
        GenericCommand cmd = null;

        try {
            MongoConnection con = morphium.getDriver().getPrimaryConnection(null);
            cmd = new GenericCommand(con).addKey("listShards", 1).setDb("morphium_test").setColl("admin");
            int msgid = con.sendCommand(cmd);
            Map<String, Object> state = con.readSingleAnswer(msgid);
            log.info("Sharding state: " + Utils.toJsonString(state));
        } catch (MorphiumDriverException e) {
            log.info("Not sharded, it seems");
            return;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }

        try {
            MongoConnection con = morphium.getDriver().getPrimaryConnection(null);
            cmd = new GenericCommand(con).addKey("shardCollection", "morphium_test." + morphium.getMapper().getCollectionName(UncachedObject.class)).addKey("key", Doc.of("_id", "hashed"))
            .setDb("admin");
            int msgid = con.sendCommand(cmd);
            // con.sendCommand((Doc.of("shardCollection", ("morphium_test." + morphium.getMapper().getCollectionName(UncachedObject.class)),
            //                    "key", UtilsMap.of("_id", "hashed"), "$db", "admin")));
            Map<String, Object> state = con.readSingleAnswer(msgid);
            log.info("Sharding state: " + Utils.toJsonString(state));
        } catch (MorphiumDriverException e) {
            log.error("Sharding is enabled, but morphium_test sharding is not it seems");

            if (cmd != null) {
                cmd.releaseConnection();
            }

            return;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }

        createUncachedObjects(morphium, 10000);
        UncachedObject uc = new UncachedObject("toReplace", 1234);
        morphium.store(uc, morphium.getMapper().getCollectionName(UncachedObject.class), null);
        Thread.sleep(100);
        uc.setStrValue("again");
        morphium.store(uc, morphium.getMapper().getCollectionName(UncachedObject.class), null);
        morphium.reread(uc, morphium.getMapper().getCollectionName(UncachedObject.class));
        assert(uc.getStrValue().equals("again"));
        uc = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(42).get();
        uc.setStrValue("another value");
        morphium.store(uc, morphium.getMapper().getCollectionName(UncachedObject.class), null);
        Thread.sleep(100);
        morphium.reread(uc, morphium.getMapper().getCollectionName(UncachedObject.class));
        assert(uc.getStrValue().equals("another value"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void shardingStringIdReplacementTest(Morphium morphium) throws Exception  {
        GenericCommand cmd = null;

        try {
            MongoConnection con = morphium.getDriver().getPrimaryConnection(null);
            cmd = new GenericCommand(con).addKey("listShards", 1).setDb("morphium_test").setColl("admin");
            int msgid = con.sendCommand(cmd);
            Map<String, Object> state = con.readSingleAnswer(msgid);
            log.info("Sharding state: " + Utils.toJsonString(state));
        } catch (MorphiumDriverException e) {
            log.info("Not sharded, it seems");
            return;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }

        cmd = null;

        try {
            MongoConnection con = morphium.getDriver().getPrimaryConnection(null);
            cmd = new GenericCommand(con).addKey("shardCollection", "morphium_test." + morphium.getMapper().getCollectionName(UncachedObject.class)).addKey("key", Doc.of("_id", "hashed"))
            .setDb("admin");
            int msgid = con.sendCommand(cmd);
            Map<String, Object> state = con.readSingleAnswer(msgid);
            log.info("Sharding state: " + Utils.toJsonString(state));
        } catch (MorphiumDriverException e) {
            log.error("Sharding is enabled, but morphium_test sharding is not it seems");
            return;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }

        StringIdTestEntity uc = new StringIdTestEntity();
        uc.value = "test123e";
        morphium.store(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class), null);
        Thread.sleep(100);
        uc.value = "again";
        morphium.store(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class), null);
        morphium.reread(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class));
        assert(uc.value.equals("again"));
        uc = new StringIdTestEntity();
        uc.value = "test123";
        morphium.store(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class), null);
        uc = morphium.createQueryFor(StringIdTestEntity.class).f(StringIdTestEntity.Fields.value).eq("test123").get();
        uc.value = "another value";
        morphium.store(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class), null);
        Thread.sleep(100);
        morphium.reread(uc, morphium.getMapper().getCollectionName(StringIdTestEntity.class));
        assert(uc.value.equals("another value"));
    }

    @Entity
    public static class StringIdTestEntity {
        @Id
        public String id;
        public String value;

        public enum Fields {
            value, id
        }
    }

}
