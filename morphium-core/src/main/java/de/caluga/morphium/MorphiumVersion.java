package de.caluga.morphium;

import java.io.InputStream;
import java.util.Properties;

/**
 * Provides the Morphium version at runtime.
 * <p>
 * The version is read from {@code morphium-version.properties}, a resource filtered by Maven
 * with the project version at build time. Reading a classpath resource (instead of the jar
 * manifest) also works in GraalVM native images, provided the resource is included in the image.
 */
public final class MorphiumVersion {
    public static final String UNKNOWN_VERSION = "unknown";
    private static final String VERSION = load();

    private MorphiumVersion() {
    }

    public static String getVersion() {
        return VERSION;
    }

    private static String load() {
        try (InputStream in = MorphiumVersion.class.getResourceAsStream("/morphium-version.properties")) {
            if (in == null) {
                return UNKNOWN_VERSION;
            }
            Properties p = new Properties();
            p.load(in);
            String version = p.getProperty("version", UNKNOWN_VERSION);
            // running from sources without resource filtering leaves the placeholder unresolved
            if (version.contains("${")) {
                return UNKNOWN_VERSION;
            }
            return version;
        } catch (Exception e) {
            return UNKNOWN_VERSION;
        }
    }
}
