package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


public class MapAsEntityTests extends MorphiumTestBase {

    @Test
    public void mapQueryTest() throws Exception {
        createUncachedObjects(100);
        Query<Map<String, Object>> q = morphium.createMapQuery(morphium.getMapper().getCollectionName(UncachedObject.class));
        List<Map<String, Object>> lst = q.asMapList();
        assertThat(lst.size()).isEqualTo(100);
    }

    @Test
    public void mapQueryTestIterable() throws Exception {
        createUncachedObjects(100);
        Query<Map<String, Object>> q = morphium.createMapQuery(morphium.getMapper().getCollectionName(UncachedObject.class));
        int count = 0;
        for (Map<String, Object> o : q.asMapIterable()) {
            count++;
            assertThat(q != null).isTrue();

        }
        assertThat(count).isEqualTo(100);
    }
}
