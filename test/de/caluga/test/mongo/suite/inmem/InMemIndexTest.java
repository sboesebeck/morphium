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
        assertThat(idx.size()).isEqualTo(0); //_id index + indexes for str_value etc.
    }


    @Test
    public void indexedReadWriteTest() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            morphium.store(new UncachedObject("str_" + i, i * i));
        }
        long dur = System.currentTimeMillis() - start;
        log.info("Storing 10000 ucs took " + dur + "ms");
        //Storing 10000 ucs took 2860ms

        start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(i * i).get();
        }
        dur = System.currentTimeMillis() - start;
        log.info("Searching for 10000 ucs took " + dur + "ms");
        //Searching for 10000 ucs took 11230ms
    }
}
