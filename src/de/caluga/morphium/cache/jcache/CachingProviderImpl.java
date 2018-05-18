package de.caluga.morphium.cache.jcache;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.18
 * Time: 08:19
 * <p>
 * TODO: Add documentation here
 */
public class CachingProviderImpl implements CachingProvider {
    private Map<URI, CacheManagerImpl> managers = new ConcurrentHashMap<>();

    public Collection<CacheManagerImpl> getCacheManagers() {
        return managers.values();
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        if (!managers.containsKey(uri)) {
            CacheManagerImpl cacheManager = new CacheManagerImpl(properties);
            cacheManager.setCachingProvider(this);
            cacheManager.setUri(uri);
            managers.putIfAbsent(uri, cacheManager);
        }
        return managers.get(uri);
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return this.getClass().getClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        try {
            return new URI("default");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Properties getDefaultProperties() {
        return new Properties();
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager(uri, classLoader, getDefaultProperties());
    }

    @Override
    public CacheManager getCacheManager() {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader(), getDefaultProperties());
    }

    @Override
    public void close() {
        for (Map.Entry<URI, CacheManagerImpl> e : managers.entrySet()) {
            e.getValue().close();
        }
    }

    @Override
    public void close(ClassLoader classLoader) {
        close();
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        getCacheManager(uri, classLoader).close();
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }
}
