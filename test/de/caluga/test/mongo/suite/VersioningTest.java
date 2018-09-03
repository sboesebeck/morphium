package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.VersionedEntity;
import org.junit.Test;

public class VersioningTest extends MongoTest {


    @Test
    public void simpleVersionTest() throws Exception {
        VersionedEntity ve = new VersionedEntity("ve1", 1);

        morphium.store(ve);
        assert (ve.getTheVersionNumber() > 0);

    }


}