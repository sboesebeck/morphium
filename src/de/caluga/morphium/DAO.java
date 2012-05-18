package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.12
 * Time: 15:06
 * <p/>
 */
public abstract class DAO<T> {
    private Morphium morphium;
    private Class<T> type;

    public DAO(Morphium m, Class<T> type) {
        this.type = type;
        morphium = m;
    }

    public Query<T> getQuery() {
        return morphium.createQueryFor(type);
    }

    public Object getValue(Enum field) throws IllegalAccessException {
        return getValue(field.name());
    }
    public Object getValue(String field) throws IllegalAccessException {
        return morphium.getConfig().getMapper().getField(type, field).get(this);
    }

    public void setValue(Enum field, Object value) throws  IllegalAccessException {
        setValue(field.name(),value);
    }

    public void setValue(String field, Object value) throws IllegalAccessException {
        morphium.getConfig().getMapper().getField(type, field).set(this, value);
    }

    public boolean existsField(String field) {
        return morphium.getConfig().getMapper().getField(type, field) != null;
    }

}
