/**
 * Marker package for the {@code morphium-spring-boot-starter} artifact.
 *
 * <p>This starter is intentionally an empty jar: it exists only to pull in the
 * {@code morphium-spring-boot-autoconfigure} module and its transitive dependencies
 * with a single Maven coordinate, following Spring Boot's own starter convention
 * (see {@code spring-boot-starter-web} and similar). All auto-configuration classes,
 * {@code @ConfigurationProperties}, and repository infrastructure live in
 * {@code morphium-spring-boot-autoconfigure} instead.
 *
 * <p>This package-info exists solely so that {@code maven-javadoc-plugin} and
 * {@code maven-source-plugin} have at least one compilation unit to process —
 * Maven Central requires a {@code -sources.jar} and {@code -javadoc.jar} for every
 * published artifact, and both plugins otherwise silently produce no jar at all
 * when a module's source tree is completely empty.
 */
package de.caluga.morphium.spring.starter;
