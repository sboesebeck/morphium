package de.caluga.test.morphium.query;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.query.FieldNames;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FieldNamesLambdaTest {
    private static Morphium morphium;

    @BeforeAll
    public static void setup() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.connectionSettings().setDatabase("testdb");
        cfg.driverSettings().setDriverName("InMemDriver");
        morphium = new Morphium(cfg);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    public void testFieldNameExtraction() {
        String f1 = FieldNames.of(UncachedObject::getStrValue);
        String f2 = FieldNames.of(UncachedObject::getCounter);
        Assertions.assertEquals("strValue", f1);
        Assertions.assertEquals("counter", f2);
    }

    @Test
    public void testQueryWithLambdaField() {
        // store a couple of entities
        for (int i = 0; i < 5; i++) {
            UncachedObject u = new UncachedObject("val" + (i % 2), i);
            morphium.store(u);
        }

        // query using lambda field name helper
        var q = morphium.createQueryFor(UncachedObject.class)
                .f(FieldNames.of(UncachedObject::getStrValue)).eq("val1");

        List<UncachedObject> res = q.asList();
        Assertions.assertFalse(res.isEmpty(), "Expected results when querying by strValue");

        // check it also works with sort using the helper
        var q2 = morphium.createQueryFor(UncachedObject.class)
                .f(FieldNames.of(UncachedObject::getCounter)).gt(0)
                .sort("-" + FieldNames.of(UncachedObject::getCounter));
        var res2 = q2.asList();
        Assertions.assertTrue(res2.size() >= 2);
        Assertions.assertTrue(res2.get(0).getCounter() >= res2.get(res2.size() - 1).getCounter());
    }
}

