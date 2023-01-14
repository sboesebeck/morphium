package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class InMemIndexTest extends MorphiumInMemTestBase {

    @Test
    public void indexCreationTest() throws Exception {
//        createUncachedObjects(100);
//        List<Map<String, Object>> idx = morphium.getDriver().getIndexes(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class));
//        assertNotNull(idx);
//        assertThat(idx.size()).isGreaterThan(1); //_id index + indexes for str_value etc.
//        List<Map<String, Object>> indexDefinition = ((InMemoryDriver) morphium.getDriver()).getIndexes(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class));
//        assertThat(indexDefinition.size()).isGreaterThan(0);
//        Map idIndex = ((InMemoryDriver) morphium.getDriver()).getIndexDataForCollection(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class), "_id");
//        assertEquals(100,idIndex.size());
//        Map counterIndex = ((InMemoryDriver) morphium.getDriver()).getIndexDataForCollection(morphium.getDatabase(), morphium.getMapper().getCollectionName(UncachedObject.class), "counter");
//        assertEquals(100,idIndex.size());
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
            assertEquals(1, lst.size());
            UncachedObject uc = lst.get(0);
            assertEquals(i * i, uc.getCounter());
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

        assertFalse(lst.isEmpty());
        assertEquals(0, lst.get(1).getCounter() % 5);
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
            assertEquals(3, lst.size());
        }
        long dur = System.currentTimeMillis() - start;
        log.info("No index: " + dur + " ms");


        morphium.ensureIndex(UncachedObject.class, UtilsMap.of("counter", 1, "str_value", 1));

        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            List<UncachedObject> lst = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).lt(100)
                    .f(UncachedObject.Fields.strValue).in(Arrays.asList("str_1", "str_2", "str_5", "str_42", "str_245", "str_99", "str_58")).asList();
            assertEquals(3, lst.size());
        }
        dur = System.currentTimeMillis() - start;
        log.info("with index: " + dur + " ms");
    }

    @Test
    public void deleteTest() throws Exception {

        var uc=new UncachedObject("String",1000);
        morphium.insert(uc);
        boolean ex=false;
        try {
           morphium.insert(uc);
        } catch (Exception e){
            //expected
            ex=true;
        }
        assertTrue(ex);
        assertEquals(1,morphium.createQueryFor(UncachedObject.class).countAll());
        morphium.delete(uc);
        assertEquals(0,morphium.createQueryFor(UncachedObject.class).countAll());
        ex=false;
        try {
            morphium.insert(uc);
        } catch (Exception e){
            e.printStackTrace();
            ex=true;
        }
        assertFalse(ex);

    }
}
