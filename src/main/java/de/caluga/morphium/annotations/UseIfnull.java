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
 * Controls whether null values should be stored in the database for this field.
 *
 * <h3>Behavior:</h3>
 * <ul>
 *   <li><b>Without @UseIfNull:</b> When a field value is null, the field is omitted from the database document
 *       (not stored at all). When reading from the database, if the field is missing, the entity's default
 *       value is preserved.</li>
 *   <li><b>With @UseIfNull:</b> When a field value is null, the field is stored in the database with a null
 *       value. When reading from the database, the field will be set to null even if the entity class has
 *       a default value.</li>
 * </ul>
 *
 * <h3>Example:</h3>
 * <pre>{@code
 * @Entity
 * public class MyEntity {
 *     private String regularField;      // null values not stored in DB
 *
 *     @UseIfNull
 *     private String nullableField;     // null values ARE stored in DB
 *
 *     private Integer counter = 0;      // default value; if field missing in DB, stays 0
 *
 *     @UseIfNull
 *     private Integer nullCounter = 0;  // default value; if null in DB, becomes null
 * }
 * }</pre>
 *
 * <h3>Use Cases:</h3>
 * This annotation is useful for:
 * <ul>
 *   <li>Sharding keys that must always be present (even as null) for MongoDB sharding</li>
 *   <li>Distinguishing between "not set" (field missing) and "explicitly set to null"</li>
 *   <li>Ensuring consistent document structure across all records</li>
 *   <li>Fields that participate in sparse indexes where null values are significant</li>
 * </ul>
 *
 * @author stephan
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface UseIfNull {

}
