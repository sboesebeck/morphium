package de.caluga.test.mongo.suite.base;

import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 12.07.12
 * Time: 16:46
 * <p>
 */
@Tag("core")
public class SkipLimitTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void skipTest(Morphium morphium) throws Exception  {
        createUncachedObjects(morphium, 100);
        Thread.sleep(500);
        UncachedObject o = morphium.createQueryFor(UncachedObject.class).f("counter").lt(100).skip(10).sort("counter").get();
        assertEquals (10, o.getCounter(), "Counter is " + o.getCounter());

    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void limitTest(Morphium morphium) throws Exception  {
        createUncachedObjects(morphium, 100);
        Thread.sleep(500);

        List<UncachedObject> l = morphium.createQueryFor(UncachedObject.class).f("counter").lt(100).limit(10).sort("counter").asList();
        assertEquals(10, l.size() );
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void skipLimitTest(Morphium morphium) throws Exception  {
        createUncachedObjects(morphium, 100);
        Thread.sleep(500);
        List<UncachedObject> l = morphium.createQueryFor(UncachedObject.class).f("counter").lt(100).skip(50).limit(10).sort("counter").asList();
        assertEquals (10, l.size());
        assertEquals(50, l.get(0).getCounter() );
    }

}
