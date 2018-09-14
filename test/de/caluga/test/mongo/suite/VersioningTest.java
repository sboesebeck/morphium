package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.VersionedEntity;
import org.junit.Test;

public class VersioningTest extends MongoTest {


    @Test(expected = IllegalArgumentException.class)
    public void simpleVersionTest() throws Exception {
        VersionedEntity ve = new VersionedEntity("ve1", 1);

        morphium.store(ve);
        assert (ve.getTheVersionNumber() > 0);

        long v=ve.getTheVersionNumber();

        ve.setCounter(ve.getCounter()+1);
        morphium.store(ve);
        assert(ve.getTheVersionNumber()==v+1L);

        //forcing versioning error
        ve.setCounter(34);
        ve.setTheVersionNumber(323);
        morphium.store(ve);
        assert(ve!=null);
    }


}