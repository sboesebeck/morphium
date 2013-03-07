package de.caluga.morphium;

import de.caluga.morphium.query.Query;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.12
 * Time: 15:06
 * <p/>
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class DAO<T> {
    private Morphium morphium;
    private Class<? extends T> type;
    private AnnotationAndReflectionHelper ah = new AnnotationAndReflectionHelper();

    public DAO(Morphium m, Class<? extends T> type) {
        this.type = type;
        morphium = m;
    }

    public Query<T> getQuery() {
        return morphium.createQueryFor(type);
    }

    public Object getValue(Enum field, T obj) throws IllegalAccessException {
        return getValue(field.name(), obj);
    }

    public Object getValue(String field, T obj) throws IllegalAccessException {
        return ah.getField(type, field).get(obj);
    }

    public void setValue(Enum field, Object value, T obj) throws IllegalAccessException {
        setValue(field.name(), value, obj);
    }

    public void setValue(String field, Object value, T obj) throws IllegalAccessException {
        ah.getField(type, field).set(obj, value);
    }

    public boolean existsField(String field) {
        return ah.getField(type, field) != null;
    }

}
