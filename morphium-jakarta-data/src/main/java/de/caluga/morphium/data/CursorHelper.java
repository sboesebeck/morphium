package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import jakarta.data.page.PageRequest;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for cursor-based (keyset) pagination.
 * <p>
 * Called from {@link AbstractMorphiumRepository#doFindAllCursored}, {@link FindMethodBridge}, and
 * {@link JdqlMethodBridge} whenever a {@code @Find} or {@code @Query} method returns a
 * {@code CursoredPage<T>}. It has two responsibilities: turning a sort order into a MongoDB
 * {@code $or}/comparison condition that continues a result set after (or before) a given cursor
 * ({@link #applyCursorCondition}), and extracting new cursors from the returned entities for the
 * next/previous page ({@link #extractCursor}, {@link #extractCursors}). It does not parse queries
 * itself; the caller has already built the base {@link Query} and just needs cursor support added.
 */
public final class CursorHelper {

    private CursorHelper() {}

    public record SortSpec(String javaField, boolean ascending) {}

    /**
     * Parses an orderBySpec string ("field1:ASC,field2:DESC") into SortSpec list.
     *
     * @param orderBySpec the encoded sort spec, e.g. {@code "field1:ASC,field2:DESC"}; may be
     *                     {@code null} or empty
     * @return the parsed sort specs, empty if {@code orderBySpec} was {@code null} or empty
     */
    public static List<SortSpec> parseSortSpecs(String orderBySpec) {
        List<SortSpec> specs = new ArrayList<>();
        if (orderBySpec == null || orderBySpec.isEmpty()) return specs;
        for (String part : orderBySpec.split(",")) {
            String[] fieldAndDir = part.split(":");
            specs.add(new SortSpec(fieldAndDir[0], !"DESC".equals(fieldAndDir[1])));
        }
        return specs;
    }

    /**
     * Extracts a cursor from an entity based on the sort fields.
     * The cursor contains the values of the sort fields in order.
     *
     * @param entity      the entity to extract the cursor values from
     * @param sortFields  the Java field names, in sort order, that make up the cursor key
     * @param morphium    the Morphium instance (for field resolution/reflection)
     * @param entityClass the entity class
     * @return a cursor holding the values of {@code sortFields} for {@code entity}, in order
     * @throws IllegalStateException if a field value cannot be extracted from the entity
     */
    @SuppressWarnings("unchecked")
    public static PageRequest.Cursor extractCursor(Object entity, List<String> sortFields,
                                                    Morphium morphium, Class<?> entityClass) {
        Object[] values = new Object[sortFields.size()];
        for (int i = 0; i < sortFields.size(); i++) {
            values[i] = getFieldValue(entity, sortFields.get(i), morphium, entityClass);
        }
        return PageRequest.Cursor.forKey(values);
    }

    /**
     * Extracts cursors for all entities in a content list.
     *
     * @param content     the entities to extract cursors from
     * @param sortFields  the Java field names, in sort order, that make up the cursor key
     * @param morphium    the Morphium instance (for field resolution/reflection)
     * @param entityClass the entity class
     * @return one cursor per entity in {@code content}, in the same order
     * @throws IllegalStateException if a field value cannot be extracted from an entity
     */
    public static List<PageRequest.Cursor> extractCursors(List<?> content, List<String> sortFields,
                                                           Morphium morphium, Class<?> entityClass) {
        List<PageRequest.Cursor> cursors = new ArrayList<>(content.size());
        for (Object entity : content) {
            cursors.add(extractCursor(entity, sortFields, morphium, entityClass));
        }
        return cursors;
    }

    /**
     * Applies a cursor condition to the query for keyset pagination.
     * <p>
     * For CURSOR_NEXT with sort [amount ASC, id ASC] and cursor [200, "abc"]:
     * <pre>
     * $or: [
     *   { amount: { $gt: 200 } },
     *   { amount: 200, _id: { $gt: "abc" } }
     * ]
     * </pre>
     * For CURSOR_PREVIOUS, comparison operators are inverted and sort direction is flipped.
     *
     * @param query       the query to add the cursor condition to (modified in place)
     * @param cursor      the cursor to continue from
     * @param sortSpecs   the sort fields defining the keyset, in sort order
     * @param morphium    the Morphium instance (for field resolution)
     * @param entityClass the entity class
     * @param isForward   true for {@code CURSOR_NEXT}, false for {@code CURSOR_PREVIOUS}
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void applyCursorCondition(Query query, PageRequest.Cursor cursor,
                                             List<SortSpec> sortSpecs,
                                             Morphium morphium, Class entityClass,
                                             boolean isForward) {
        if (sortSpecs == null || sortSpecs.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cursor-based pagination requires a non-empty sort order to define the keyset; got no sort fields");
        }

        List orQueries = new ArrayList();

        for (int i = 0; i < sortSpecs.size(); i++) {
            Query sub = morphium.createQueryFor(entityClass);

            // All preceding fields must be equal
            for (int j = 0; j < i; j++) {
                String mongoField = resolveMongoField(morphium, entityClass, sortSpecs.get(j).javaField());
                sub.f(mongoField).eq(cursor.get(j));
            }

            // The i-th field uses a comparison operator
            SortSpec spec = sortSpecs.get(i);
            String mongoField = resolveMongoField(morphium, entityClass, spec.javaField());
            Object cursorValue = cursor.get(i);

            // Determine comparison direction:
            // CURSOR_NEXT + ASC → $gt, CURSOR_NEXT + DESC → $lt
            // CURSOR_PREVIOUS + ASC → $lt, CURSOR_PREVIOUS + DESC → $gt
            boolean useGt = isForward == spec.ascending();

            if (useGt) {
                sub.f(mongoField).gt(cursorValue);
            } else {
                sub.f(mongoField).lt(cursorValue);
            }

            orQueries.add(sub);
        }

        query.or(orQueries);
    }

    /**
     * Applies sort to a query, inverting direction for CURSOR_PREVIOUS.
     *
     * @param query       the query to sort (modified in place)
     * @param sortSpecs   the sort fields to apply, in sort order
     * @param morphium    the Morphium instance (for field resolution)
     * @param entityClass the entity class
     * @param isForward   true for {@code CURSOR_NEXT}/offset paging, false for {@code CURSOR_PREVIOUS}
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void applySort(Query query, List<SortSpec> sortSpecs,
                                  Morphium morphium, Class entityClass,
                                  boolean isForward) {
        Map<String, Integer> sortMap = new LinkedHashMap<>();
        for (SortSpec spec : sortSpecs) {
            String mongoField = resolveMongoField(morphium, entityClass, spec.javaField());
            boolean ascending = isForward ? spec.ascending() : !spec.ascending();
            sortMap.put(mongoField, ascending ? 1 : -1);
        }
        query.sort(sortMap);
    }

    /**
     * Reads the value of the given Java field from an entity via reflection.
     *
     * @param entity        the entity instance
     * @param javaFieldName the Java field name
     * @param morphium      the Morphium instance (for field resolution)
     * @param entityClass   the entity class
     * @return the field value
     * @throws IllegalStateException if the field cannot be found or read
     */
    @SuppressWarnings("unchecked")
    private static Object getFieldValue(Object entity, String javaFieldName,
                                         Morphium morphium, Class<?> entityClass) {
        try {
            Field field = morphium.getARHelper().getField(entityClass, javaFieldName);
            if (field != null) {
                field.setAccessible(true);
                return field.get(entity);
            }
        } catch (Exception e) {
            // fallback below
        }
        throw new IllegalStateException(
                "Cannot extract cursor value for field '" + javaFieldName + "' on " + entityClass.getName());
    }

    /**
     * Resolves a Java field name to its MongoDB field name, falling back to the Java name.
     *
     * @param morphium      the Morphium instance
     * @param entityClass   the entity class
     * @param javaFieldName the Java field name
     * @return the MongoDB field name, or {@code javaFieldName} if it cannot be resolved
     */
    @SuppressWarnings("unchecked")
    static String resolveMongoField(Morphium morphium, Class<?> entityClass, String javaFieldName) {
        try {
            return morphium.getARHelper().getMongoFieldName(entityClass, javaFieldName);
        } catch (Exception e) {
            return javaFieldName;
        }
    }
}
