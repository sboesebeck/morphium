package de.caluga.test.mongo.suite;

import de.caluga.morphium.ConfigManager;
import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 04.05.12
 * Time: 12:05
 * <p/>
 * TODO: Add documentation here
 */
public class ConfigManagerTest extends MongoTest {
    @Test
    public void storeConfig() throws Exception {
        ConfigManager cfg = MorphiumSingleton.get().getConfigManager();
        cfg.addSetting("test", "Test Value");
        cfg.addSetting("test2", "Test Value");

        String set = cfg.getSetting("test");
        assert (set.equals("Test Value")) : "Value wrong";

        cfg.addSetting("test", "Test Value2");
        cfg.addSetting("test", "Test Value3");

        set = cfg.getSetting("test");
        assert (set.equals("Test Value3")) : "Value wrong";

    }


    @Test
    public void mapConfigTest() throws Exception {
        ConfigManager cfg = MorphiumSingleton.get().getConfigManager();
        Map<String, String> tst = new HashMap<String, String>();
        tst.put("k1", "v1");
        tst.put("k2", "v2");
        cfg.addSetting("mtest", tst);

        Map<String, String> set = cfg.getMapSetting("mtest");
        assert (set.get("k1").equals("v1")) : "K1 not stored";
    }


    @Test
    public void listConfigTest() throws Exception {
        ConfigManager cfg = MorphiumSingleton.get().getConfigManager();
        List<String> tst = new ArrayList<String>();
        tst.add("V1");
        tst.add("v2");
        tst.add("and another test");

        cfg.addSetting("ltest", tst);

        List<String> set = cfg.getListSetting("ltest");
        assert (set.size() == 3 && set.get(0).equals("V1") && set.get(1).equals("v2")) : "List not stored correctly";
    }
}
