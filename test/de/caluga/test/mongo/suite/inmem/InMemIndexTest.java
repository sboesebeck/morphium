package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemIndexTest extends MorphiumInMemTestBase {

    @Test
    public void indexCreationTest() throws Exception {
//        createUncachedObjects(100);
//        List<Map<String, Object>> idx = morphium.getDriver().getIndexes(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class));
//        assertThat(idx).isNotNull();
//        assertThat(idx.size()).isGreaterThan(1); //_id index + indexes for str_value etc.
//        List<Map<String, Object>> indexDefinition = ((InMemoryDriver) morphium.getDriver()).getIndexes(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class));
//        assertThat(indexDefinition.size()).isGreaterThan(0);
//        Map idIndex = ((InMemoryDriver) morphium.getDriver()).getIndexDataForCollection(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class), "_id");
//        assertThat(idIndex.size()).isEqualTo(100);
//        Map counterIndex = ((InMemoryDriver) morphium.getDriver()).getIndexDataForCollection(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class), "counter");
//        assertThat(idIndex.size()).isEqualTo(100);
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
        List<UncachedObject> lst = null;

        start = System.currentTimeMillis();

        for (int i = 0; i < 10000; i++) {
            lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).eq(i * i).asList();
            assertThat(lst.size()).isEqualTo(1);
            UncachedObject uc = lst.get(0);
            assertThat(uc.getCounter()).isEqualTo(i * i);
        }
        dur = System.currentTimeMillis() - start;
        log.info("Searching for 10000 ucs took " + dur + "ms");
        //no index search: Searching for 10000 ucs took 11230ms

        //Cache warming?!?!
        for (int i = 0; i < 100; i++) {
            lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).mod(3, 0)
                    .f(UncachedObject.Fields.counter).lte(300).asList();
        }

        //trying partial index match:
        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).mod(5, 0)
                    .f(UncachedObject.Fields.counter).lte(300).asList();
        }
        dur = System.currentTimeMillis() - start;
        log.info("Mod-Query took " + dur + "ms");

        assertThat(lst).isNotEmpty();
        assertThat(lst.get(1).getCounter() % 5).isEqualTo(0);
        //forcing index miss
        ((InMemoryDriver) morphium.getDriver()).getIndexes(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class)).clear();
        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).mod(5, 0)
                    .f(UncachedObject.Fields.counter).lte(300).asList();
        }
        dur = System.currentTimeMillis() - start;
        log.info("Mod-Query (noindex) took " + dur + "ms");
    }


    @Test
    public void createIndexTest() throws Exception {
        for (int i = 0; i < 10000; i++) {
            morphium.store(new UncachedObject("str_" + i % 100, i * 10));
        }
        //no index
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(100)
                    .f(UncachedObject.Fields.strValue).in(Arrays.asList("str_1", "str_2", "str_5", "str_42", "str_245", "str_99", "str_58")).asList();
            assertThat(lst.size()).isEqualTo(3);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("No index: " + dur + " ms");


        morphium.ensureIndex(UncachedObject.class, UtilsMap.of("counter", 1, "str_value", 1));

        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(100)
                    .f(UncachedObject.Fields.strValue).in(Arrays.asList("str_1", "str_2", "str_5", "str_42", "str_245", "str_99", "str_58")).asList();
            assertThat(lst.size()).isEqualTo(3);
        }
        dur = System.currentTimeMillis() - start;
        log.info("with index: " + dur + " ms");
    }
}
