package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.CreationTime;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 19.11.13
 * Time: 12:29
 * To change this template use File | Settings | File Templates.
 */
@Tag("core")
public class CheckForNewTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testCheckForNew(Morphium morphium) {
        //although checkfornew is enabled, it will not update created
        //as the @CreationTime annotation disables it
        morphium.getConfig().objectMappingSettings().setCheckForNew(true);
        morphium.delete(morphium.createQueryFor(TestID.class));

        TestID tst = new TestID();
        tst.theId = "1";
        tst.theValue = "value";
        morphium.store(tst);
        assertNull(tst.created);


        tst = new TestID();
        tst.theId = "2";
        tst.theValue = "value2";
        morphium.store(tst);
        assert (tst.created == null);

        tst = new TestID();
        tst.theId = "2";
        tst.theValue = "value";
        morphium.store(tst);
        assert (tst.created == null);

        tst.created = new Date();
        Date cr = tst.created;

        morphium.store(tst);
        assert (cr.equals(tst.created));

        morphium.reread(tst);
        assert (cr.equals(tst.created));

        morphium.getConfig().objectMappingSettings().setCheckForNew(false);
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testCheckForNew2(Morphium morphium) throws Exception  {
        morphium.getConfig().objectMappingSettings().setCheckForNew(true);
        morphium.dropCollection(TestID2.class);

        TestID2 tst = new TestID2();
        tst.theId = "1";
        tst.theValue = "value";
        morphium.store(tst);
        assertNotNull(tst.created);
        ;


        tst = new TestID2();
        tst.theId = "2";
        tst.theValue = "value2";
        morphium.store(tst);
        assertNotNull(tst.created);
        ;


        morphium.getConfig().objectMappingSettings().setCheckForNew(false);
    }

    @Entity
    @CreationTime(checkForNew = true)
    public static class TestID2 {
        @Id
        public String theId;
        public String theValue;

        @CreationTime
        public Date created;

    }


    @Entity
    @CreationTime(checkForNew = false)
    public static class TestID {
        @Id
        public String theId;
        public String theValue;

        @CreationTime
        public Date created;

    }
}
