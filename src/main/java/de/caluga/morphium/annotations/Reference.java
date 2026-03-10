package de.caluga.morphium.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:18
 * <p>
 * Mark this field as a reference to another mongo object.
 * In mongo only the id will be stored here.
 * If <code>automaticStore</code> is true (default), objects will be stored if not already done.
 */


@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Reference {
    String fieldName() default ".";

    boolean automaticStore() default true;

    boolean lazyLoading() default false;

    String targetCollection() default ".";

    /**
     * If true, referenced entities will be deleted when the parent entity is deleted.
     * Only applies to entity-based remove(Object) calls, not query-based deletes.
     * Cycle-safe: circular cascadeDelete references will not cause infinite loops.
     */
    boolean cascadeDelete() default false;

    /**
     * If true, referenced entities that are no longer referenced after an update
     * will be automatically deleted. Only applies when updating existing entities
     * (entities with a non-null ID).
     */
    boolean orphanRemoval() default false;
}
