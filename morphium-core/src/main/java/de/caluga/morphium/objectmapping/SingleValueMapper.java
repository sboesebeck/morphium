package de.caluga.morphium.objectmapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;

/**
 * A value object with ONE component is a scalar in the document (MOR-035): {@code record Email(String value)} is stored as {@code "a@example.org"}, not as
 * {@code {value: "a@example.org"}}, and a query or an update with the value object takes the scalar.
 *
 * <pre>{@code
 * morphium.getMapper().registerCustomMapperFor(Email.class, SingleValueMapper.of(Email.class));
 * }</pre>
 *
 * Only a record with exactly one component is accepted: for a class the mapping of its one value to the constructor is a convention that the class has to state itself, which
 * is what implementing {@link MorphiumTypeMapper} is for. A container of such values (a list of them) is the case of GitHub #334 and not covered here.
 *
 * @param <T> the value object
 */
public final class SingleValueMapper<T> implements MorphiumTypeMapper<T> {

    private final Class<T> type;
    private final RecordComponent component;
    private final Constructor<T> constructor;

    private SingleValueMapper(Class<T> type) {
        this.type = type;
        RecordComponent[] components = type.getRecordComponents();

        if (components == null || components.length != 1) {
            throw new IllegalArgumentException(type.getName() + " is not a record with exactly one component; a single-value mapper needs one");
        }

        this.component = components[0];

        try {
            this.constructor = type.getDeclaredConstructor(component.getType());
            this.constructor.setAccessible(true);
            this.component.getAccessor().setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("The canonical constructor of " + type.getName() + " is missing", e);
        }
    }

    /** the mapper of a record with one component */
    public static <T> SingleValueMapper<T> of(Class<T> type) {
        return new SingleValueMapper<>(type);
    }

    @Override
    public Object marshall(T o) {
        try {
            return component.getAccessor().invoke(o);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not read the component " + component.getName() + " of " + type.getName(), e);
        }
    }

    @Override
    public T unmarshall(Object d) {
        if (d == null) {
            return null;
        }

        Object value = d;

        // a document that an older writer made of it: {value: scalar}
        if (d instanceof java.util.Map<?, ?> map && map.size() <= 2 && map.containsKey(component.getName())) {
            value = map.get(component.getName());
        }

        try {
            return constructor.newInstance(coerce(value, component.getType()));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not create " + type.getName() + " from " + d, e);
        }
    }

    private static Object coerce(Object value, Class<?> target) {
        if (value instanceof Number n) {
            if (target == long.class || target == Long.class) {
                return n.longValue();
            }

            if (target == int.class || target == Integer.class) {
                return n.intValue();
            }

            if (target == double.class || target == Double.class) {
                return n.doubleValue();
            }
        }

        return value;
    }
}
