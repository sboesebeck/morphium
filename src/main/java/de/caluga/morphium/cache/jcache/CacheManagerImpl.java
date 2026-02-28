package de.caluga.morphium.cache.jcache;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.18
 * Time: 08:21
 * <p>
 * TODO: Add documentation here
 */
public class CacheManagerImpl implements CacheManager {
    private CachingProvider cachingProvider;
    private URI uri;
    private ClassLoader classLoader;
    private Properties properties;

    private final Map<String, Cache> caches = new ConcurrentHashMap<>();

    public CacheManagerImpl(Properties settings) {


    }

    public void setCachingProvider(CachingProvider cachingProvider) {
        this.cachingProvider = cachingProvider;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return cachingProvider;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }


    public Collection<Cache> getCaches() {
        return caches.values();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        CacheImpl cache = new CacheImpl();
        cache.setCacheManager(this);
        caches.put(cacheName, cache);
        cache.setName(cacheName);
        return (Cache<K, V>) cache;

    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {

        if (!caches.containsKey(cacheName)) {
            createCache(cacheName, null);
        }

        return (Cache<K, V>) caches.get(cacheName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        return getCache(cacheName, (Class<K>) Object.class, (Class<V>) Object.class);
    }

    @Override
    public Iterable<String> getCacheNames() {
        return caches.keySet();
    }

    @Override
    public void destroyCache(String cacheName) {
        getCache(cacheName).clear();
        caches.remove(cacheName);
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {

    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return clazz.cast(this);
    }


    private Class<?> getTypeClass() throws ClassNotFoundException {
        return Class.forName(getURI().toString());
    }

}
