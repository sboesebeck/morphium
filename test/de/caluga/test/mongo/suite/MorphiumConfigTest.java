package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

import java.util.Properties;

/**
 * User: Stephan Bösebeck
 * Date: 30.07.13
 * Time: 14:31
 * <p/>
 * TODO: Add documentation here
 */
public class MorphiumConfigTest extends MongoTest {
    @Test
    public void testToString() throws Exception {
        String cfg = MorphiumSingleton.get().getConfig().toString();
        log.info("Config: " + cfg);

        MorphiumConfig c = MorphiumConfig.createFromJson(cfg);
        assert (c.getAdr().size() == 3);
    }

    @Test
    public void testDefaultProps() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig();
        Properties p = cfg.asProperties();
        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
        }
        p.store(System.out, "testproperties");

        String cfgStr = cfg.toString();
        log.info("Got: " + cfgStr);

    }

    @Test
    public void partialJsonTest() throws Exception {
        String json = "{ \"hosts\":\"mongo1:27018, mongo2:27099\", \"database\" : \"testdb\", \"safe_mode\" : true , \"global_fsync\" : false , \"globalJ\" : false , \"write_timeout\" : 9990 }";
        MorphiumConfig cfg = MorphiumConfig.createFromJson(json);
        assert (cfg.getWriteTimeout() == 9990);
        assert (cfg.isSafeMode());
        assert (cfg.getDatabase().equals("testdb"));
        assert (cfg.getAdr().size() == 2);
        assert (cfg.getAdr().get(0).getPort() == 27018);
        assert (cfg.getAdr().get(1).getPort() == 27099);

    }

    @Test
    public void testHosts() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.addHost("localhost:9999");
        cfg.addHost("localhost", 1000);
        assert (cfg.getAdr().size() == 2);

        cfg.setHosts("localhost:9999,localhost:2222,localhost:2344");
        assert (cfg.getAdr().size() == 3);

        cfg.setHosts("localhost,localhost,localhost,localhost", "1, 2,   3,4");
        assert (cfg.getAdr().size() == 4);

    }


    @Test
    public void partialPropsTest() throws Exception {
        Properties p = new Properties();
        p.put("maximumRetriesAsyncWriter", "10");
        p.put("socketTimeout", "1000");
        p.put("database", "thingy");
        p.put("hosts", "localhost:27017");

        MorphiumConfig cfg = MorphiumConfig.fromProperties(p);
        assert (cfg.getAdr().size() == 1);
        assert (cfg.getDatabase().equals("thingy"));
        assert (cfg.getSocketTimeout() == 1000);
    }

    @Test
    public void testToProperties() throws Exception {
        Properties p = MorphiumSingleton.get().getConfig().asProperties();
        for (Object k : p.keySet()) {
            log.info("Key: " + k + " Value: " + p.get(k));
        }
        p.store(System.out, "testproperties");

        MorphiumConfig cfg = MorphiumConfig.fromProperties(p);
        assert (cfg.getDatabase().equals(MorphiumSingleton.get().getConfig().getDatabase()));
        assert (cfg.getAdr().size() == 3);
        assert (cfg.getQueryClass() != null);
    }
}
