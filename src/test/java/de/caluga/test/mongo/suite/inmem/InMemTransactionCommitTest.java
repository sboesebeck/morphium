package de.caluga.test.mongo.suite.inmem;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("inmemory")
public class InMemTransactionCommitTest extends MorphiumInMemTestBase {

    @Test
    public void commitReplacesDatabaseState() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        createUncachedObjects(3);
        assertEquals(3, morphium.createQueryFor(UncachedObject.class).countAll());

        morphium.startTransaction();
        // drop collection within transaction
        morphium.dropCollection(UncachedObject.class);
        morphium.commitTransaction();

        // After commit, collection should remain dropped (count 0)
        assertEquals(0, morphium.createQueryFor(UncachedObject.class).countAll());
    }
}

