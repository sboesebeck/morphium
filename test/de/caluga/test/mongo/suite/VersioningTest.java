package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.UncachedObject;
import de.caluga.test.mongo.suite.data.VersionedEntity;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VersioningTest extends MorphiumTestBase {


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
        for (int i = 0; i < 100; i++) lst.add(new VersionedEntity("str_value" + i, i));

        morphium.storeList(lst);

        Thread.sleep(200);

        lst.get(0).setTheVersionNumber(1234);
        morphium.storeList(lst);
    }

    @Test
    public void updateVersionTest() throws Exception {
        for (int i = 0; i < 100; i++) morphium.store(new VersionedEntity("value" + i, i));

        Thread.sleep(100);

        morphium.set(morphium.createQueryFor(VersionedEntity.class).f(VersionedEntity.Fields.strValue).eq("value10"), UncachedObject.Fields.counter, 1234);
        Thread.sleep(100);

        VersionedEntity ve = morphium.createQueryFor(VersionedEntity.class).f(VersionedEntity.Fields.strValue).eq("value10").get();
        assert (ve.getTheVersionNumber() == 2);
        ve = morphium.createQueryFor(VersionedEntity.class).f(VersionedEntity.Fields.strValue).eq("value11").get();
        assert (ve.getTheVersionNumber() == 1);
    }


    @Test
    public void testing() throws Exception {
        VersionedEntity ve = new VersionedEntity("str_value", 1);
        morphium.store(ve);
        Thread.sleep(500);
        VersionedEntity v2 = morphium.findById(VersionedEntity.class, ve.getMorphiumId());
        v2.setStrValue("test");
        morphium.store(v2);

    }
}