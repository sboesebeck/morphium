/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.annotations.lifecycle;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;

/**
 * will be called, before a single object is updated (this obect to be exact)
 * Unfortuately, it can't be called when a query based update is executed
 */
@Target({METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PreUpdate {

}
