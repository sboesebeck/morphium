package de.caluga.test.mongo.suite;

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
public class ServerStatusTest extends MongoTest {

    @Test
    public void getServerStatus() throws Exception {
        Morphium m = morphium;
        Map res = m.execCommand("serverStatus");

        System.out.println(res.toString());

    }
}
