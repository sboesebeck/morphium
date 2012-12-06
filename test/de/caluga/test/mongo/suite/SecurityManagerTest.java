package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.secure.MongoSecurityManager;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 03.04.12
 * Time: 22:30
 * <p/>
 */
public class SecurityManagerTest extends MongoTest {

    @Test
    public void denialTest() {
        MongoSecurityManager mgr = MorphiumSingleton.getConfig().getSecurityMgr();
        MorphiumSingleton.getConfig().setSecurityMgr(new DenyingSecurityManager());

        UncachedObject o = new UncachedObject();
        o.setCounter(12);
        boolean th = true;
        try {
            MorphiumSingleton.get().store(o); //should throw an exception
            th = false;
        } catch (Exception e) {
            System.out.println("got exception - good!");
        }

        assert (th) : "Exception not thrown!!!";

        th = true;
        try {
            MorphiumSingleton.get().createQueryFor(CachedObject.class).f("counter").eq(15).asList(); //should throw an exception
            th = false;
        } catch (Exception e) {
            System.out.println("got exception for reading - good!");
        }

        assert (th) : "Exception not thrown!!!";
        //Need to reset - otherwise test will fail (clearing of collections)
        MorphiumSingleton.getConfig().setSecurityMgr(mgr);
    }


    @Test
    public void simpleSecurityMgrTest() {
        MongoSecurityManager mgr = MorphiumSingleton.getConfig().getSecurityMgr();
        SimpleSecurityManager smgr = new SimpleSecurityManager();
        smgr.login("Default User");

        MorphiumSingleton.getConfig().setSecurityMgr(smgr);

        MorphiumSingleton.get().readAll(UncachedObject.class); //should be allowed
        MorphiumSingleton.get().store(new UncachedObject()); //should be allowed
        boolean th = true;
        try {
            MorphiumSingleton.get().dropCollection(UncachedObject.class);
            th = false;
        } catch (Exception e) {
            System.out.println("Got exception - good");
        }
        assert (th) : "did not get Exception????";

        th = true;
        try {
            MorphiumSingleton.get().readAll(CachedObject.class);
            th = false;
        } catch (Exception e) {
            System.out.println("Got exception - good");
        }
        assert (th) : "did not get Exception????";


        //Need to reset - otherwise test will fail (clearing of collections)
        MorphiumSingleton.getConfig().setSecurityMgr(mgr);
    }
}
