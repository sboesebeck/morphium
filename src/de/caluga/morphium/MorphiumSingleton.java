package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 03.05.12
 * Time: 10:50
 * <p/>
 * TODO: Add documentation here
 */
public class MorphiumSingleton {
    private static MorphiumConfig config;
    private static volatile Morphium instance;

    /**
     * set configuration for MongoDbLayer
     *
     * @param cfg
     * @see MorphiumConfig
     */
    public static void setConfig(MorphiumConfig cfg) {
        if (config != null) {
            throw new RuntimeException("Morphium already configured!");
        }
        config = cfg;
    }

    /**
     * returns true, if layer was configured yet
     *
     * @return
     */
    public static boolean isConfigured() {
        return config != null;
    }

    public static boolean isInitialized() {
        return instance != null;
    }

    public static MorphiumConfig getConfig() {
        return config;
    }

    public static void reset() {
        config = null;
        instance = null;
    }


    /**
     * threadsafe Singleton implementation.
     *
     * @return Morphium instance
     */
    public static Morphium get() {
        if (instance == null) {
            if (config == null) {
                throw new RuntimeException("MongoDbLayer not configured!");
            }
            synchronized (Morphium.class) {
                if (instance == null) {
                    if (config.getConfigManager() == null) {
                        config.setConfigManager(new ConfigManagerImpl());
                    }
                    instance = new Morphium(config);

                    instance.getConfig().getConfigManager().setTimeout(config.getConfigManagerCacheTimeout());
                }
            }
        }
        return instance;
    }
}
