package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.secure.MongoSecurityManager;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 03.04.12
 * Time: 22:30
 * <p/>
 * TODO: Add documentation here
 */
public class SecurityManagerTest extends MongoTest {

    @Test
    public void denialTest() {
        MongoSecurityManager mgr = Morphium.getConfig().getSecurityMgr();
        Morphium.getConfig().setSecurityMgr(new DenyingSecurityManager());

        UncachedObject o = new UncachedObject();
        o.setCounter(12);
        boolean th = true;
        try {
            Morphium.get().store(o); //should throw an exception
            th = false;
        } catch (Exception e) {
            System.out.println("got exception - good!");
        }

        assert (th) : "Exception not thrown!!!";

        th = true;
        try {
            Morphium.get().createQueryFor(CachedObject.class).f("counter").eq(15).asList(); //should throw an exception
            th = false;
        } catch (Exception e) {
            System.out.println("got exception for reading - good!");
        }

        assert (th) : "Exception not thrown!!!";
        //Need to reset - otherwise test will fail (clearing of collections)
        Morphium.getConfig().setSecurityMgr(mgr);
    }


    @Test
    public void simpleSecurityMgrTest() {
        MongoSecurityManager mgr = Morphium.getConfig().getSecurityMgr();
        SimpleSecurityManager smgr = new SimpleSecurityManager();
        smgr.login("Default User");

        Morphium.getConfig().setSecurityMgr(smgr);

        Morphium.get().readAll(UncachedObject.class); //should be allowed
        Morphium.get().store(new UncachedObject()); //should be allowed
        boolean th = true;
        try {
            Morphium.get().dropCollection(UncachedObject.class);
            th = false;
        } catch (Exception e) {
            System.out.println("Got exception - good");
        }
        assert (th) : "did not get Exception????";

        th = true;
        try {
            Morphium.get().readAll(CachedObject.class);
            th = false;
        } catch (Exception e) {
            System.out.println("Got exception - good");
        }
        assert (th) : "did not get Exception????";


        //Need to reset - otherwise test will fail (clearing of collections)
        Morphium.getConfig().setSecurityMgr(mgr);
    }
}
