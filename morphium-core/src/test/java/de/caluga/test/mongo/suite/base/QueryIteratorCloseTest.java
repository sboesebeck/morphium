package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.MorphiumIterator;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class QueryIteratorCloseTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void closeWithoutConsumingDoesNotOpenCursor(Morphium morphium) {
        for (int i = 1; i <= 5; i++) {
            morphium.store(new UncachedObject("v" + i, i));
        }

        // Creating an iterator and closing it without calling hasNext()/next()
        // must not wastefully open a MongoDB cursor
        MorphiumIterator<UncachedObject> it = morphium.createQueryFor(UncachedObject.class).asIterable();
        it.close(); // before fix: NPE or unnecessary cursor creation
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    void doubleCloseAfterExhaustion(Morphium morphium) {
        for (int i = 1; i <= 3; i++) {
            morphium.store(new UncachedObject("v" + i, i));
        }

        MorphiumIterator<UncachedObject> it = morphium.createQueryFor(UncachedObject.class).asIterable();
        // exhaust the iterator
        while (it.hasNext()) {
            it.next();
        }
        // close after exhaustion — connection already released by hasNext()
        it.close();
        // second close must not throw
        it.close();
    }
}
