package de.caluga.morphium.cache.jcache;

import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 18.05.18
 * Time: 13:35
 * <p>
 * TODO: Add documentation here
 */
public class CacheLoaderImpl implements CacheLoader<String, Object> {
    @Override
    public Object load(String key) throws CacheLoaderException {
        return null;
    }

    @Override
    public Map<String, Object> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
        return null;
    }
}
