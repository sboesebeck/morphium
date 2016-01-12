package de.caluga.test.mongo.suite;/**
 * Created by stephan on 12.01.16.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.test.mongo.suite.data.CachedObject;

/**
 * TODO: Add Documentation here
 **/
public class ExitTest {

    public static void main(String args[]) throws Exception {
        // Morphium m=new Morphium("localhost:27017","morphium-test");

        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("morphium-test");
        cfg.setHostSeed("localhost:27017,localhost:27018,localhost:27019");
        Morphium m = new Morphium(cfg);
        System.out.println("Connection opened...");

        m.createQueryFor(CachedObject.class).countAll();
        m.close();
        System.out.println("All closed");
    }
}
