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

@Tag("core")
public class BasicAdminTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void readPreferenceTest(Morphium morphium) {
        ReadPreferenceLevel.NEAREST.setPref(ReadPreference.nearest());
        assert(ReadPreferenceLevel.NEAREST.getPref().getType().equals(ReadPreference.nearest().getType()));
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void getDatabaseListTest(Morphium morphium) {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try(morphium) {
            morphium.save(new UncachedObject("str", 1));
            List<String> dbs = morphium.listDatabases();
            assertNotNull(dbs);
            assert(dbs.size() != 0);

            for (String s : dbs) {
                log.info("Got DB: " + s);
            }
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void reconnectDatabaseTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try(morphium) {
            List<String> dbs = morphium.listDatabases();
            assertNotNull(dbs);
            assertThat(dbs.size()).isNotEqualTo(0);

            // Limit to first 5 databases to avoid timeout with many test databases
            int count = 0;
            for (String s : dbs) {
                if (count++ >= 5) break;
                log.info("Got DB: " + s);
                morphium.reconnectToDb(s);
                log.info("Logged in...");
                Thread.sleep(100);
            }

            morphium.reconnectToDb("morphium_test");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void listCollections(Morphium morphium) {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try(morphium) {
            UncachedObject u = new UncachedObject("test", 1);
            morphium.store(u);
            List<String> cols = morphium.listCollections();
            assertNotNull(cols);
            ;
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testExistst(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try(morphium) {
            morphium.store(new UncachedObject("value", 123));
            assertTrue(morphium.getDriver().exists(morphium.getConfig().connectionSettings().getDatabase()));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void existsTest(Morphium morphium) throws Exception {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try(morphium) {
            for (int i = 1; i <= 10; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            long s = System.currentTimeMillis();

            while (TestUtils.countUC(morphium) < 10) {
                Thread.sleep(100);
                assert(System.currentTimeMillis() - s < morphium.getConfig().connectionSettings().getMaxWaitTime());
            }

            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").exists().f("str_value").eq("Uncached 1");
            long c = q.countAll();
            s = System.currentTimeMillis();

            while (c != 1) {
                c = q.countAll();
                Thread.sleep(100);
                assert(System.currentTimeMillis() - s < morphium.getConfig().connectionSettings().getMaxWaitTime());
            }

            assert(c == 1) : "Count wrong: " + c;
            UncachedObject o = q.get();
            s = System.currentTimeMillis();

            while (o == null) {
                Thread.sleep(100);
                assert(System.currentTimeMillis() - s < morphium.getConfig().connectionSettings().getMaxWaitTime());
                o = q.get();
            }

            assert(o.getCounter() == 1);
        }
    }
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void exitsDropTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.store(new UncachedObject("str", 123));
            Thread.sleep(100);
            assertTrue(morphium.exists(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class)));
            morphium.dropCollection(UncachedObject.class);
            Thread.sleep(100);
            assertFalse(morphium.exists(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class)));
        }
    }
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void notExistsTest(Morphium morphium) {
        String tstName = new Object() {} .getClass().getEnclosingMethod().getName();
        log.info("Running test " + tstName + " with " + morphium.getDriver().getName());

        try(morphium) {
            for (int i = 1; i <= 10; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setStrValue("Uncached " + i);
                morphium.store(o);
            }

            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("counter").notExists().f("str_value").eq("Uncached 1");
            long c = q.countAll();
            assertEquals(0, c);
        }
    }



}
