package de.caluga.test.mongo.suite.bson;/**
 * Created by stephan on 04.11.15.
 */

import java.util.logging.Level;

/**
 * TODO: Add Documentation here
 **/
public class BaseTest {


    @org.junit.BeforeClass
    public static void setUpClass() throws Exception {
        System.setProperty("morphium.log.level", "4");
        System.setProperty("morphium.log.synced", "true");
        System.setProperty("morphium.log.file", "-");
        java.util.logging.Logger l = java.util.logging.Logger.getGlobal();
        l.setLevel(Level.SEVERE);
    }


}
