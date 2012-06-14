package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.PartiallyUpdateable;
import de.caluga.morphium.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 14.06.12
 * Time: 22:38
 * <p/>
 * TODO: Add documentation here
 */
public class PartialUpdateTests extends MongoTest {
    public static final int NO_OBJECTS = 100;

    /**
     * test checking for partial updates
     */
    @Test
    public void testPartialUpdates() {
        List<Object> tst = new ArrayList<Object>();
        int cached = 0;
        int uncached = 0;
        for (int i = 0; i < NO_OBJECTS; i++) {
            if (Math.random() < 0.5) {
                cached++;
                CachedObject c = new CachedObject();
                c.setValue("List Test!");
                c.setCounter(11111);
                tst.add(c);
            } else {
                uncached++;
                UncachedObject uc = new UncachedObject();
                uc.setValue("List Test uc");
                uc.setCounter(22222);
                tst.add(uc);
            }
        }
        log.info("Writing " + cached + " Cached and " + uncached + " uncached objects!");
        MorphiumSingleton.get().storeList(tst);
        waitForWrites();

        Object o = tst.get((int) (Math.random() * tst.size()));
        if (o instanceof CachedObject) {
            log.info("Got a cached Object");
            ((CachedObject) o).setValue("Updated!");
            MorphiumSingleton.get().updateUsingFields(o, "value");
            waitForWrites();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
            log.info("Cached object altered... look for it");
            Query<CachedObject> c = MorphiumSingleton.get().createQueryFor(CachedObject.class);
            CachedObject fnd = (CachedObject) c.f("_id").eq(((CachedObject) o).getId()).get();
            assert (fnd.getValue().equals("Updated!")) : "Value not changed? " + fnd.getValue();

        } else {
            log.info("Got a uncached Object");
            ((UncachedObject) o).setValue("Updated!");
            MorphiumSingleton.get().updateUsingFields(o, "value");
            log.info("uncached object altered... look for it");
            Query<UncachedObject> c = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
            UncachedObject fnd = (UncachedObject) c.f("_id").eq(((UncachedObject) o).getMongoId()).get();
            assert (fnd.getValue().equals("Updated!")) : "Value not changed? " + fnd.getValue();
        }


        UncachedObject uo = new UncachedObject();
        uo = MorphiumSingleton.get().createPartiallyUpdateableEntity(uo);
        assert (uo instanceof PartiallyUpdateable) : "Created proxy incorrect";
        uo.setValue("A TEST");
        List<String> alteredFields = ((PartiallyUpdateable) uo).getAlteredFields();
        for (String f : alteredFields) {
            log.info("Field altered: " + f);
        }

        assert (alteredFields.contains("value")) : "Field not set?";

    }


}
