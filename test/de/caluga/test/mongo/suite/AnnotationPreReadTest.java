package de.caluga.test.mongo.suite;

import de.caluga.morphium.EntityCache;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * Created by stephan on 25.09.14.
 */
public class AnnotationPreReadTest extends MongoTest {

    private Logger log = Logger.getLogger(AnnotationPreReadTest.class);

    @Test
    public void testClassPathTraversal() throws Exception {

        long start = System.currentTimeMillis();
        EntityCache f = new EntityCache();
        long dur = System.currentTimeMillis() - start;
        log.info("Duration: " + dur);


        for (String typeId : f.getEntityByTypeId().keySet()) {
            log.info("TypeId: " + typeId + "  => " + f.getEntityByTypeId().get(typeId));
        }

        for (Class e : f.getEnumlist()) {
            log.info("Enumeration: " + e.getSimpleName());
        }

    }


}
