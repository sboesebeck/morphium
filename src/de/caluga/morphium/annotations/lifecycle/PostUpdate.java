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
 * will be called, after this object was updated by a direct call to update
 * won't be called, when updates are done query based.
 */
@Target({METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PostUpdate {

}
