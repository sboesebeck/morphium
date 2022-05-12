package de.caluga.test.mongo.suite.base;

import de.caluga.test.mongo.suite.data.UncachedObject;
import de.caluga.test.mongo.suite.data.VersionedEntity;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class VersioningTest extends MorphiumTestBase {


    @Test(expected = Exception.class)
    public void simpleVersionTest() throws Exception {
        VersionedEntity ve = new VersionedEntity("ve1", 1);

        morphium.store(ve);
        assertThat(ve.getTheVersionNumber()).isGreaterThan(0);

        long v=ve.getTheVersionNumber();

        ve.setCounter(ve.getCounter()+1);
        morphium.store(ve);
        assertThat(ve.getTheVersionNumber()).isEqualTo(v + 1L);

        //forcing versioning error
        ve.setCounter(34);
        ve.setTheVersionNumber(323);
        morphium.store(ve);
        assertThat(ve).isNotNull();
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
        long s = System.currentTimeMillis();
        while (morphium.createQueryFor(VersionedEntity.class).countAll() != 100) {
            Thread.sleep(100);
            assertThat(System.currentTimeMillis() - s).isLessThan(morphium.getConfig().getMaxWaitTime());
        }

        morphium.set(morphium.createQueryFor(VersionedEntity.class).f(VersionedEntity.Fields.strValue).eq("value10"), UncachedObject.Fields.counter, 1234);
        s = System.currentTimeMillis();
        VersionedEntity entity = morphium.createQueryFor(VersionedEntity.class).f(VersionedEntity.Fields.strValue).eq("value10").get();
        while (entity != null && entity.getTheVersionNumber() != 2) {
            Thread.sleep(100);
            assertThat(System.currentTimeMillis() - s).isLessThan(morphium.getConfig().getMaxWaitTime());
            entity = morphium.createQueryFor(VersionedEntity.class).f(VersionedEntity.Fields.strValue).eq("value10").get();
        }
        Thread.sleep(100);
        assertThat(morphium.createQueryFor(VersionedEntity.class).f("strValue").eq("value10").countAll()).isEqualTo(1);
        VersionedEntity ve = morphium.createQueryFor(VersionedEntity.class).f(VersionedEntity.Fields.strValue).eq("value10").get();
        assertThat(ve.getTheVersionNumber()).isEqualTo(2);
        ve = morphium.createQueryFor(VersionedEntity.class).f(VersionedEntity.Fields.strValue).eq("value11").get();
        assertThat(ve.getTheVersionNumber()).isEqualTo(1);
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