package de.caluga.morphium.data;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Converts the result of a query to the shape that the repository method declares. The bridges answer with a {@link List}, a {@link Stream} or a single entity; a
 * method that returns an array of the entity ({@code Product[] findByPriceLessThan(...)}) gets one from here (MOR-080).
 */
public final class ResultShapes {

    private ResultShapes() {
    }

    /**
     * @param result        a List, a Stream, another Collection, an array of the right type, or null
     * @param componentType the component type of the array that the method returns
     * @return an array of {@code componentType} with the elements of the result; null for null
     */
    public static Object toArray(Object result, Class<?> componentType) {
        if (result == null) {
            return null;
        }

        if (result.getClass().isArray()) {
            return result;
        }

        List<?> elements;

        if (result instanceof Collection<?> collection) {
            elements = List.copyOf(collection);
        } else if (result instanceof Stream<?> stream) {
            try (stream) {
                elements = stream.collect(Collectors.toList());
            }
        } else {
            elements = List.of(result);
        }

        Object array = Array.newInstance(componentType, elements.size());

        for (int i = 0; i < elements.size(); i++) {
            Array.set(array, i, elements.get(i));
        }

        return array;
    }
}
