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
 * Controls bidirectional null value handling for this field during both serialization and deserialization.
 *
 * <h3>Behavior Summary:</h3>
 * This annotation determines whether a field participates in null value handling in both directions:
 * writing to the database and reading from it.
 *
 * <h3>Serialization (Writing to DB):</h3>
 * <ul>
 *   <li><b>Without @UseIfNull:</b> When a field value is null, the field is omitted from the database document
 *       (not stored at all).</li>
 *   <li><b>With @UseIfNull:</b> When a field value is null, the field is stored in the database with an explicit
 *       null value.</li>
 * </ul>
 *
 * <h3>Deserialization (Reading from DB):</h3>
 * <ul>
 *   <li><b>Without @UseIfNull:</b> If the field is missing from the database, the entity's default value is preserved.
 *       <strong>If the field exists in DB with a null value, it is ignored and the default value is preserved.</strong>
 *       This protects fields from unwanted null contamination.</li>
 *   <li><b>With @UseIfNull:</b> If the field is missing from the database, the entity's default value is preserved.
 *       If the field exists in DB with a null value, the field will be set to null, overriding any default value.</li>
 * </ul>
 *
 * <h3>Example:</h3>
 * <pre>{@code
 * @Entity
 * public class MyEntity {
 *     // Field without @UseIfNull - protected from nulls
 *     private String regularField;      // null values not stored; null from DB ignored
 *
 *     // Field with @UseIfNull - accepts nulls bidirectionally
 *     @UseIfNull
 *     private String nullableField;     // null values stored; null from DB accepted
 *
 *     // Field with default value, without @UseIfNull
 *     private Integer counter = 42;     // Missing from DB: stays 42
 *                                       // null in DB: stays 42 (protected!)
 *
 *     // Field with default value, with @UseIfNull
 *     @UseIfNull
 *     private Integer nullCounter = 99; // Missing from DB: stays 99
 *                                       // null in DB: becomes null (accepted)
 * }
 * }</pre>
 *
 * <h3>Protection from Null Contamination:</h3>
 * Fields WITHOUT @UseIfNull are protected from null values in the database:
 * <pre>{@code
 * // If MongoDB document has: { counter: null }
 * // Without @UseIfNull: counter stays at default (42)
 * // With @UseIfNull: counter becomes null
 * }</pre>
 *
 * <h3>Use Cases:</h3>
 * This annotation is useful for:
 * <ul>
 *   <li>Sharding keys that must always be present (even as null) for MongoDB sharding</li>
 *   <li>Distinguishing between "not set" (field missing) and "explicitly set to null"</li>
 *   <li>Ensuring consistent document structure across all records</li>
 *   <li>Fields that participate in sparse indexes where null values are significant</li>
 *   <li>Protecting fields from null contamination during data migrations or manual edits</li>
 *   <li>Maintaining data integrity when documents are modified outside the application</li>
 * </ul>
 *
 * @author stephan
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface UseIfNull {

}
