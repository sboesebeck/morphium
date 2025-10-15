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
 * Protects a field from null values during deserialization from the database.
 *
 * <h3>Behavior Summary:</h3>
 * By default, Morphium accepts null values from the database and sets fields to null, even if they have
 * default values. This annotation changes that behavior for specific fields that need protection from
 * null contamination.
 *
 * <h3>Default Behavior (Without @IgnoreNullFromDB):</h3>
 * <ul>
 *   <li><b>Serialization (Writing to DB):</b> When a field value is null, the field is stored in the
 *       database with an explicit null value.</li>
 *   <li><b>Deserialization (Reading from DB):</b> If the field exists in DB with a null value, the field
 *       will be set to null, overriding any default value.</li>
 * </ul>
 *
 * <h3>With @IgnoreNullFromDB:</h3>
 * <ul>
 *   <li><b>Serialization (Writing to DB):</b> When a field value is null, the field is omitted from the
 *       database document (not stored at all).</li>
 *   <li><b>Deserialization (Reading from DB):</b> If the field exists in DB with a null value, it is
 *       ignored and the entity's default value is preserved. This protects fields from unwanted null
 *       contamination.</li>
 * </ul>
 *
 * <h3>Example:</h3>
 * <pre>{@code
 * @Entity
 * public class MyEntity {
 *     // Field without @IgnoreNullFromDB - standard behavior
 *     private String regularField;      // null values stored; null from DB accepted
 *
 *     // Field with @IgnoreNullFromDB - protected from nulls
 *     @IgnoreNullFromDB
 *     private String protectedField;    // null values not stored; null from DB ignored
 *
 *     // Field with default value, without @IgnoreNullFromDB
 *     private Integer counter = 42;     // Missing from DB: stays 42
 *                                       // null in DB: becomes null
 *
 *     // Field with default value, with @IgnoreNullFromDB
 *     @IgnoreNullFromDB
 *     private Integer protectedCounter = 99; // Missing from DB: stays 99
 *                                            // null in DB: stays 99 (protected!)
 * }
 * }</pre>
 *
 * <h3>Protection from Null Contamination:</h3>
 * Fields WITH @IgnoreNullFromDB are protected from null values in the database:
 * <pre>{@code
 * // If MongoDB document has: { counter: null }
 * // Without @IgnoreNullFromDB: counter becomes null
 * // With @IgnoreNullFromDB: counter stays at default (42)
 * }</pre>
 *
 * <h3>Use Cases:</h3>
 * This annotation is useful for:
 * <ul>
 *   <li>Protecting fields from null contamination during data migrations or manual edits</li>
 *   <li>Maintaining data integrity when documents are modified outside the application</li>
 *   <li>Ensuring fields with meaningful default values are never overridden by null</li>
 *   <li>Backward compatibility when introducing new fields with defaults to existing documents</li>
 *   <li>Defensive programming against unexpected null values in the database</li>
 * </ul>
 *
 * @author stephan
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface IgnoreNullFromDB {

}
