package de.caluga.morphium.quarkus.deployment;

import io.quarkus.builder.item.SimpleBuildItem;

/**
 * Marker build item indicating that {@code @Entity}/{@code @Embedded} class names
 * have been passed to the {@link de.caluga.morphium.quarkus.MorphiumRecorder} via
 * {@code setMappedClassNames()}.
 *
 * <p>Other build steps that depend on the entity list being available at runtime
 * (e.g. migration execution) must consume this build item to guarantee correct
 * ordering of {@code @Record(RUNTIME_INIT)} bytecode blocks.
 */
public final class MorphiumEntitiesRegisteredBuildItem extends SimpleBuildItem {
}
