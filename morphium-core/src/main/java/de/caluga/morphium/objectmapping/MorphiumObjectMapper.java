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
     * Prepares the value of a WHOLE FIELD of an update document ({@code $set}, {@code $setOnInsert}, {@code $inc}, {@code $min}, ...) for the wire encoder, in the
     * shape {@code store()} writes for a field of that value (#335): a custom-mapped value ({@code LocalDate}, {@code Instant}, {@code BigDecimal},
     * {@code AtomicInteger}, ...) is the bare output of its type mapper, entities and embedded objects become documents with {@code class_name} (an entity keeps its
     * {@code _id}), enums become their constant name. The members of Lists, Sets, arrays and Maps are marshalled with {@link #marshallUpdateElement(Object)}; the
     * containers are copied (Maps with String keys, an enum key is written by name), a {@code MorphiumId} inside a container becomes an {@code ObjectId}. Everything
     * else is passed through.
     * <p>
     * The method is idempotent and never modifies its argument.
     *
     * @param value the value, may be null
     * @return the value as it can be handed to the driver
     * @throws IllegalArgumentException for a map with a key that is neither a String nor an enum
     */
    Object marshallUpdateValue(Object value);

    /**
     * As {@link #marshallUpdateValue(Object)}, for a value that is ONE ELEMENT of a container ({@code $push}, {@code $addToSet}, {@code $pull}, {@code $each} members,
     * and the target of a positional path such as {@code list.$}): it has the shape {@code store()} gives the elements of a container. A custom-mapped scalar
     * is wrapped as {@code {"value": scalar}}, a map-returning mapper gets {@code class_name}, a {@code MorphiumId} becomes an {@code ObjectId}; types that
     * the mapper writes natively ({@code String}, numbers, {@code Date}, ...) are not mapped.
     *
     * @param value the element, may be null
     * @return the element as it can be handed to the driver
     */
    default Object marshallUpdateElement(Object value) {
        return ((java.util.List<?>) marshallUpdateValue(java.util.Collections.singletonList(value))).get(0);
    }

    /**
     * The operand of a {@code $pull}: either an element (equal to the elements to remove, {@link #marshallUpdateElement(Object)}) or a CONDITION. A map without
     * {@code $}-keys is a condition on the fields of embedded documents, its values are fields ({@link #marshallUpdateValue(Object)}); a map with
     * {@code $}-keys ({@code {$gte: date}}) is a condition on the elements themselves: its operands are marshalled as fields and, if the elements are stored wrapped
     * ({@code {"value": scalar}}, i.e. the operands are custom-mapped scalars), the condition is rewritten to {@code {"value": {$gte: ...}}} so that it matches.
     */
    default Object marshallUpdatePullOperand(Object operand) {
        return marshallUpdateElement(operand);
    }

    void setAnnotationHelper(AnnotationAndReflectionHelper an);

    Morphium getMorphium();

    void setMorphium(Morphium m);


    Class<?> getClassForCollectionName(String collectionName);
}
