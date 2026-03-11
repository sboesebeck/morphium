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
 * <h3>IMPORTANT: Field Missing vs. Field = null</h3>
 * Morphium distinguishes between two scenarios when reading from the database:
 * <ul>
 *   <li><b>Field missing from DB document:</b> The field key is not present in the document at all.
 *       In this case, the Java default value is ALWAYS preserved, regardless of this annotation.</li>
 *   <li><b>Field present in DB with null value:</b> The field key exists in the document but the value is null.
 *       This is where @IgnoreNullFromDB matters - it controls whether to accept or ignore the explicit null.</li>
 * </ul>
 *
 * <h3>NOTE: Special Handling for @Id Fields</h3>
 * Fields annotated with {@link Id} are NEVER stored when null, regardless of this annotation.
 * This is fundamental MongoDB behavior - if the _id field is missing, MongoDB auto-generates it.
 * Storing _id as null would cause duplicate key errors.
 *
 * <h3>Default Behavior (Without @IgnoreNullFromDB):</h3>
 * <ul>
 *   <li><b>Serialization (Writing to DB):</b> When a field value is null, the field is stored in the
 *       database with an explicit null value.</li>
 *   <li><b>Deserialization (Reading from DB):</b>
 *       <ul>
 *         <li>Field missing from DB: Default value preserved</li>
 *         <li>Field in DB with null: Field set to null, overriding any default value</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>With @IgnoreNullFromDB:</h3>
 * <ul>
 *   <li><b>Serialization (Writing to DB):</b> When a field value is null, the field is omitted from the
 *       database document (not stored at all).</li>
 *   <li><b>Deserialization (Reading from DB):</b>
 *       <ul>
 *         <li>Field missing from DB: Default value preserved</li>
 *         <li>Field in DB with null: Null value ignored, default value preserved (protected!)</li>
 *       </ul>
 *   </li>
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
 * // Scenario 1: MongoDB document has: { counter: null }
 * // Without @IgnoreNullFromDB: counter becomes null
 * // With @IgnoreNullFromDB: counter stays at default (99)
 *
 * // Scenario 2: MongoDB document has: { }  (field missing entirely)
 * // Without @IgnoreNullFromDB: counter stays at default (42)
 * // With @IgnoreNullFromDB: counter stays at default (99)
 * // -> Both cases preserve default when field is missing!
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
