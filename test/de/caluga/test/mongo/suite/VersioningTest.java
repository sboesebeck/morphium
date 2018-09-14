package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.VersionedEntity;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VersioningTest extends MongoTest {


    @Test(expected = Exception.class)
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


    @Test(expected = Exception.class)
    public void bulkUpdateVersionTest() throws Exception {

        List<VersionedEntity> lst = new ArrayList<>();
        for (int i = 0; i < 100; i++) lst.add(new VersionedEntity("value" + i, i));

        morphium.storeList(lst);

        Thread.sleep(200);

        lst.get(0).setTheVersionNumber(1234);
        morphium.storeList(lst);
    }
}