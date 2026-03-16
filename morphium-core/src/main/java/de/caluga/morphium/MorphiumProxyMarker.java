package de.caluga.morphium;

/**
 * Marker interface implemented by ByteBuddy lazy-loading proxies.
 * Provides access to the dereferenced object behind the proxy.
 */
public interface MorphiumProxyMarker {
    Object __getDeref();
    Object __getPureDeref();
    Object __getType();
}
