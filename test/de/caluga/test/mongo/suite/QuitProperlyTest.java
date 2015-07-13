package de.caluga.test.mongo.suite;/**
 * Created by stephan on 13.07.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;

/**
 * TODO: Add Documentation here
 **/
public class QuitProperlyTest {


    public static void main(String args[]) throws Exception {
        MorphiumConfig cfg = new MorphiumConfig("holidayinsider", 10, 1000, 1000);
        cfg.addHost("localhost");
        Morphium m = new Morphium(cfg);

        m.createQueryFor(UncachedObject.class).limit(10).asList();
        m.createQueryFor(CachedObject.class).limit(10).asList();

        m.close();
        //should exit
    }
}
