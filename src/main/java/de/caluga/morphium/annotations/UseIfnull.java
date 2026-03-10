/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @Deprecated This annotation is deprecated. The default behavior has been changed to accept null values
 * from the database (which is the expected behavior for most ORMs). If you need to protect specific fields
 * from null contamination, use {@link IgnoreNullFromDB} instead.
 *
 * <h3>Migration Guide:</h3>
 * <ul>
 *   <li><b>Old behavior:</b> Fields WITHOUT @UseIfNull ignored nulls from DB; fields WITH @UseIfNull accepted them</li>
 *   <li><b>New behavior:</b> Fields accept nulls by default; use @IgnoreNullFromDB to protect from nulls</li>
 * </ul>
 *
 * <h3>How to migrate:</h3>
 * <ul>
 *   <li>Remove @UseIfNull from fields that should accept nulls (this is now the default)</li>
 *   <li>Add @IgnoreNullFromDB to fields that previously did NOT have @UseIfNull and you want to keep the protection</li>
 * </ul>
 *
 * @author stephan
 * @see IgnoreNullFromDB
 */
@Deprecated
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface UseIfnull {

}
