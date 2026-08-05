package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import jakarta.data.Order;
import jakarta.data.Sort;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps Jakarta Data {@link Order} / {@link Sort} to Morphium query sorting.
 * <p>
 * A small, self-contained mapping utility used wherever a repository method accepts a dynamic
 * {@code Order<T>} or {@code Sort<T>} parameter (as opposed to a static, method-name- or
 * annotation-derived sort order). {@link FindMethodBridge}, {@link JdqlMethodBridge}, and
 * {@link AbstractMorphiumRepository#doFindAllPaged}/{@link AbstractMorphiumRepository#doFindAllCursored}
 * inline the same field-resolution logic rather than calling this class directly in every case;
 * {@link #apply} exists as the shared entry point for callers that only need to apply Jakarta
 * Data's ordering type to a query without any other processing.
 */
public final class SortMapper {

    private SortMapper() {}

    /**
     * Applies the given Jakarta Data Order to the Morphium query.
     *
     * @param query       the Morphium query
     * @param order       the Jakarta Data order specification
     * @param morphium    the Morphium instance (for field name resolution)
     * @param entityClass the entity class
     */
    @SuppressWarnings("unchecked")
    public static void apply(Query<?> query, Order<?> order, Morphium morphium, Class<?> entityClass) {
        if (order == null || order.sorts().isEmpty()) return;

        Map<String, Integer> sortMap = new LinkedHashMap<>();
        for (Sort<?> sort : order.sorts()) {
            String mongoField = resolveMongoField(morphium, entityClass, sort.property());
            sortMap.put(mongoField, sort.isAscending() ? 1 : -1);
        }
        query.sort(sortMap);
    }

    @SuppressWarnings("unchecked")
    private static String resolveMongoField(Morphium morphium, Class<?> entityClass, String javaFieldName) {
        try {
            return morphium.getARHelper().getMongoFieldName(entityClass, javaFieldName);
        } catch (Exception e) {
            return javaFieldName;
        }
    }
}
