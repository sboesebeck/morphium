package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import org.junit.Test;

import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 29.03.15
 * Time: 22:55
 * <p/>
 * TODO: Add documentation here
 */
public class ServerStatusTest extends MorphiumTestBase {

    @Test
    public void getServerStatus() {
        Morphium m = morphium;
        Map res = m.execCommand("serverStatus");

        System.out.println(res.toString());

    }
}
