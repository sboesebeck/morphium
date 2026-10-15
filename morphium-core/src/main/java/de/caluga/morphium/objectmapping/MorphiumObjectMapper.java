package de.caluga.morphium.objectmapping;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.NameProvider;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.Map;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:24
 * <p>
 * Maps objects to Mongo
 */
public interface MorphiumObjectMapper {

    String getCollectionName(Class cls);

    Map<String, Object> serialize(Object o);

    <T> T deserialize(Class<? extends T> cls, Map<String, Object> o);

    @SuppressWarnings("RedundantThrows")
    <T> T deserialize(Class<? extends T> cls, String json) throws ParseException, IOException;

    /**
     * get current name provider for class
     *
     * @param cls - class
     * @return configured name provider in @Entity or currently set one
     */
    @SuppressWarnings("UnusedDeclaration")
    NameProvider getNameProviderForClass(Class<?> cls);

    /**
     * override settings vor name Provider from @Entity
     *
     * @param cls - class
     * @param np  the name Provider to use
     */
    void setNameProviderForClass(Class<?> cls, NameProvider np);

    <T> void registerCustomMapperFor(Class<T> cls, MorphiumTypeMapper<T> map);

    void deregisterCustomMapperFor(Class cls);

    /**
     * If a custom type mapper is registered for the value's class, marshalls the
     * value through that mapper. Otherwise returns the value unchanged.
     * This is used by the query builder to ensure query parameter values
     * (e.g. LocalDateTime) are serialized consistently with how they are stored.
     */
    Object marshallIfCustomMapped(Object value);

    /**
     * Whether a custom type mapper is registered for the given class - i.e. whether
     * {@link #marshallIfCustomMapped(Object)} would route a value of it through a mapper.
     * Lets callers branch on mappability without invoking the mapper (which may be expensive,
     * and whose result they might need to hand to a different code path anyway).
     *
     * <p>Default returns {@code false} so external implementations of this interface keep
     * compiling and degrade to the pre-#335 pass-through behaviour.
     */
    default boolean isCustomMapped(Class<?> cls) {
        return false;
    }

    /**
     * Prepares a value that is part of an update document ({@code $set}, {@code $push}, ...) for the wire encoder.
     * Only what the encoder cannot write is rewritten: entities and embedded objects become documents, enums become
     * their constant name, and types without native encoder support that have a registered type mapper
     * ({@code AtomicBoolean}, {@code AtomicInteger}, {@code AtomicLong}, {@code BigInteger}) are marshalled by that mapper.
     * Collections and arrays become lists, maps become maps with String keys, a {@code MorphiumId} inside a container becomes an
     * {@code ObjectId} (as {@code store()} writes list elements); everything else is passed through.
     * <p>
     * Documents inside a container carry {@code class_name} like the elements {@code store()} writes; a single embedded
     * value does not (single {@code @Entity} values lose their {@code _id}, as in {@code store()}). The method is
     * idempotent and never modifies its argument: containers are copied.
     *
     * @param value the value, may be null
     * @return the value as it can be handed to the driver
     * @throws IllegalArgumentException for a map with a key that is neither a String nor an enum
     */
    Object marshallUpdateValue(Object value);

    void setAnnotationHelper(AnnotationAndReflectionHelper an);

    Morphium getMorphium();

    void setMorphium(Morphium m);


    Class<?> getClassForCollectionName(String collectionName);
}
