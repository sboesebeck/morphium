package de.caluga.morphium.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * The lifecycle methods of a repository (MOR-077): {@code @Insert}, {@code @Save}, {@code @Update} and a {@code @Delete} that takes entities, under any name and with any of the shapes that
 * Jakarta Data allows. The parameter is one entity, a collection of entities, or an array or varargs of entities; the result is nothing, the entity, the entities as a list or an array, or for
 * {@code @Update} and {@code @Delete} a {@code boolean}, an {@code int} or a {@code long} (the number of entities that the method was given, as {@code Morphium.delete()} does not tell whether a document was
 * there). The work is done by the {@code doXxx()} methods of {@link AbstractMorphiumRepository}, which do not
 * depend on the entity of the repository: a lifecycle method can store any entity, so a repository without a primary entity can have them.
 */
public final class LifecycleBridge {

    /** the result shapes, as the caller names them */
    public static final String VOID = "VOID";
    public static final String ENTITY = "ENTITY";
    public static final String LIST = "LIST";
    public static final String ARRAY = "ARRAY";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String INT = "INT";
    public static final String LONG = "LONG";

    private LifecycleBridge() {
    }

    /** the shape that a return type asks for: {@code void}, an array, a collection, {@code boolean}, {@code int}, {@code long}, or an entity */
    public static String returnShape(Class<?> returnType) {
        if (returnType == void.class || returnType == Void.class) {
            return VOID;
        }

        if (returnType.isArray()) {
            return ARRAY;
        }

        if (Collection.class.isAssignableFrom(returnType) || Iterable.class.isAssignableFrom(returnType)) {
            return LIST;
        }

        if (returnType == boolean.class || returnType == Boolean.class) {
            return BOOLEAN;
        }

        if (returnType == int.class || returnType == Integer.class) {
            return INT;
        }

        if (returnType == long.class || returnType == Long.class) {
            return LONG;
        }

        return ENTITY;
    }

    /**
     * Runs a lifecycle method.
     *
     * @param kind     {@code INSERT}, {@code SAVE}, {@code UPDATE} or {@code DELETE}
     * @param argument the one parameter of the method: an entity, a collection, or an array
     * @param returns  the shape of the result, see {@link #returnShape}
     * @return the result in the shape that was asked for; for {@link #ARRAY} a list, which the caller turns into an array of its component type
     * @throws IllegalArgumentException if the shape does not fit the kind or the parameter ({@code ENTITY} for a collection, for example)
     */
    public static Object execute(AbstractMorphiumRepository<?, ?> repo, String kind, Object argument, String returns) {
        if (argument == null) {
            throw new IllegalArgumentException("A lifecycle method needs an entity, entities or an array; got null");
        }

        boolean many = argument instanceof Collection<?> || argument instanceof Object[];
        List<?> entities = many ? asList(argument) : List.of(argument);
        String k = kind.toUpperCase(Locale.ROOT);

        if (returns.equals(ENTITY) && many) {
            throw new IllegalArgumentException("A " + k + " method that takes several entities cannot return one entity: return a list, an array, or nothing");
        }

        List<?> result = switch (k) {
            case "INSERT" -> many ? repo.doInsertAll(entities) : List.of(repo.doInsert(argument));
            case "SAVE" -> many ? repo.doSaveAll(entities) : List.of(repo.doSave(argument));
            case "UPDATE" -> many ? repo.doUpdateAll(entities) : List.of(repo.doUpdate(argument));
            case "DELETE" -> {
                if (many) {
                    repo.doDeleteAll(entities);
                } else {
                    repo.doDelete(argument);
                }

                yield entities;
            }
            default -> throw new IllegalArgumentException("Not a lifecycle kind: " + kind);
        };

        return switch (returns) {
            case VOID -> null;
            case ENTITY -> result.get(0);
            case LIST, ARRAY -> new ArrayList<Object>(result);
            case BOOLEAN -> true;
            case INT -> result.size();
            case LONG -> (long) result.size();
            default -> throw new IllegalArgumentException("Not a result shape: " + returns);
        };
    }

    private static List<?> asList(Object argument) {
        return argument instanceof Collection<?> c ? new ArrayList<>(c) : Arrays.asList((Object[]) argument);
    }
}
