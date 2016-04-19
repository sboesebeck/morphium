package de.caluga.test.mongo.suite;

import de.caluga.morphium.driver.bson.MorphiumId;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:29
 * <p>
 * TODO: Add documentation here
 */
public class MorphiumIdTest {

    @Test
    public void threaddedCreationTest() throws Exception {
        for (int i = 0; i < 1000; i++) {
            new Thread() {
                @Override
                public void run() {
                    new MorphiumId();
                }
            }.start();
        }
    }
}
