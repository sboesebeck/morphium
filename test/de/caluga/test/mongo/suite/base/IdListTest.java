package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IdListTest extends MorphiumTestBase {

    @Test
    public void idList() throws Exception{
        createUncachedObjects(100);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        List<Object> lst = q.idList();
        assertThat(lst.size()).isEqualTo(90);
        assertThat(lst.get(0)).isInstanceOf(MorphiumId.class);
        assertThat(morphium.findById(UncachedObject.class,lst.get(0))).isInstanceOf(UncachedObject.class);
        assertThat(morphium.findById(UncachedObject.class,lst.get(0))).isNotNull();
    }
}