package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * Marker annotation indicating that this entity uses cascade operations
 * ({@code cascadeDelete} or {@code orphanRemoval} on {@link Reference} fields).
 * Without this annotation, cascade delete and orphan removal checks are skipped
 * entirely for performance reasons (analogous to {@link de.caluga.morphium.annotations.lifecycle.Lifecycle}).
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface CascadeAware {
}
