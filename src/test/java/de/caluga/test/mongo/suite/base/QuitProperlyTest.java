package de.caluga.test.mongo.suite.base;/**
 * Created by stephan on 13.07.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;

/**
 * TODO: Add Documentation here
 **/
public class QuitProperlyTest {


    public static void main(String[] args) {
        MorphiumConfig cfg = new MorphiumConfig("morphium-test", 10, 1000, 1000);
        cfg.clusterSettings().addHostToSeed("localhost");
        Morphium m = new Morphium(cfg);

        m.createQueryFor(UncachedObject.class).limit(10).asList();
        m.createQueryFor(CachedObject.class).limit(10).asList();

        m.close();
        //should exit
    }
}
