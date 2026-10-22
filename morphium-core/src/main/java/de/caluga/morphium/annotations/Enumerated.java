package de.caluga.morphium.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * How the enum of a field, or the enums in a collection or a map value of a field, are stored (MOR-093), like {@code @Enumerated} of JPA. Without it the setting
 * {@code objectMappingSettings().setEnumType(...)} decides, which is {@link EnumType#STRING} unless it is changed. A document is read correctly whatever form it has: a String is the name of a
 * constant, a number is an ordinal.
 * <p>
 * The keys of a map are always stored by name, as a key has to be a String.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Enumerated {
    EnumType value() default EnumType.STRING;
}
