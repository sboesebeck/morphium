package de.caluga.test.mongo.suite.inmem;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemIndexTest extends MorphiumInMemTestBase {

    @Test
    public void indexCreationTest() throws Exception {
        createUncachedObjects(100);
        List<Map<String, Object>> idx = morphium.getDriver().getIndexes(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class));
        assertThat(idx).isNotNull();
        assertThat(idx.size()).isGreaterThan(1); //_id index + indexes for str_value etc.
    }


    @Test
    public void indexedReadWriteTest() throws Exception {

        for (int i = 0; i < 1000; i++) {
            morphium.store(new UncachedObject("str_" + i, i * i));
        }
    }
}
