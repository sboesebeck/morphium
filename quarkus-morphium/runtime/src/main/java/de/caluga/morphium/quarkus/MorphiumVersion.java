/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.caluga.morphium.quarkus;

import java.io.InputStream;
import java.util.Properties;

/**
 * Provides the quarkus-morphium extension, Morphium core, and Jakarta Data versions.
 * Values are read from {@code META-INF/morphium-version.properties} which is
 * populated by Maven resource filtering at build time.
 */
public final class MorphiumVersion {

    private static final String UNKNOWN = "unknown";
    private static final String EXTENSION_VERSION;
    private static final String MORPHIUM_VERSION;
    private static final String JAKARTA_DATA_VERSION;

    static {
        Properties props = new Properties();
        try (InputStream is = MorphiumVersion.class.getClassLoader()
                .getResourceAsStream("META-INF/morphium-version.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (Exception ignored) {
            // fall through — versions stay "unknown"
        }
        EXTENSION_VERSION = props.getProperty("extension.version", UNKNOWN);
        MORPHIUM_VERSION = props.getProperty("morphium.version", UNKNOWN);
        JAKARTA_DATA_VERSION = props.getProperty("jakarta.data.version", UNKNOWN);
    }

    private MorphiumVersion() {}

    /** Returns the quarkus-morphium extension version (e.g. {@code "1.0.1-SNAPSHOT"}). */
    public static String extensionVersion() {
        return EXTENSION_VERSION;
    }

    /** Returns the Morphium core library version (e.g. {@code "6.2.1"}). */
    public static String morphiumVersion() {
        return MORPHIUM_VERSION;
    }

    /** Returns the Jakarta Data API version (e.g. {@code "1.0.0"}). */
    public static String jakartaDataVersion() {
        return JAKARTA_DATA_VERSION;
    }
}
