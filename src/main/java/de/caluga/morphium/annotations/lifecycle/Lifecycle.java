/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.annotations.lifecycle;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * Just a marker that this Entity does contain lifecycle method
 * Lifecycle methods won't be called, if this annotation is not present
 * (performance reasons).
 */
@Target({TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Lifecycle {

}
