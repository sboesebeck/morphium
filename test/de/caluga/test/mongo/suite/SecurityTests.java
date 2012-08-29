package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.secure.DefaultSecurityManager;
import de.caluga.morphium.secure.MongoSecurityException;
import de.caluga.morphium.secure.MongoSecurityManager;
import de.caluga.morphium.secure.Permission;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.08.12
 * Time: 13:58
 * <p/>
 * TODO: Add documentation here
 */
public class SecurityTests extends MongoTest {

    @Test
    public void testDenial() throws Exception {
        MorphiumSingleton.get().getConfig().setSecurityMgr(new MongoSecurityManager() {
            @Override
            public boolean checkAccess(Object obj, Permission p) throws MongoSecurityException {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean checkAccess(String domain, Permission p) throws MongoSecurityException {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean checkAccess(Class<?> cls, Permission p) throws MongoSecurityException {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Object getCurrentUserId() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });

        try {
            MorphiumSingleton.get().createQueryFor(UncachedObject.class).asList();
            assert (true) : "No exception thrown?";
        } catch (Exception e) {
            log.info("Exception caught - good");
        }

        try {
            MorphiumSingleton.get().delete(MorphiumSingleton.get().createQueryFor(UncachedObject.class));
            assert (true) : "No exception thrown?";
        } catch (Exception e) {
            log.info("Exception caught - good");
        }

        try {
            MorphiumSingleton.get().createQueryFor(UncachedObject.class).get();
            assert (true) : "No exception thrown?";
        } catch (Exception e) {
            log.info("Exception caught - good");
        }

        try {
            MorphiumSingleton.get().inc(MorphiumSingleton.get().createQueryFor(UncachedObject.class), "counter", 1);
            assert (true) : "No exception thrown?";
        } catch (Exception e) {
            log.info("Exception caught - good");
        }

        try {
            MorphiumSingleton.get().dec(MorphiumSingleton.get().createQueryFor(UncachedObject.class), "counter", 1);
            assert (true) : "No exception thrown?";
        } catch (Exception e) {
            log.info("Exception caught - good");
        }

        try {
            MorphiumSingleton.get().set(MorphiumSingleton.get().createQueryFor(UncachedObject.class), "counter", 1);
            assert (true) : "No exception thrown?";
        } catch (Exception e) {
            log.info("Exception caught - good");
        }

        try {
            MorphiumSingleton.get().push(MorphiumSingleton.get().createQueryFor(UncachedObject.class), "counter", 1);
            assert (true) : "No exception thrown?";
        } catch (Exception e) {
            log.info("Exception caught - good");
        }

        try {
            MorphiumSingleton.get().pull(MorphiumSingleton.get().createQueryFor(UncachedObject.class), "counter", 1);
            assert (true) : "No exception thrown?";
        } catch (Exception e) {
            log.info("Exception caught - good");
        }


        MorphiumSingleton.get().getConfig().setSecurityMgr(new DefaultSecurityManager());
    }
}
