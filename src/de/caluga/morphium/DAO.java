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

    public Object getValue(String field, T object) throws IllegalAccessException {
        return morphium.getConfig().getMapper().getField(type, field).get(object);
    }

    public void setValue(String field, T object, Object value) throws IllegalAccessException {
        morphium.getConfig().getMapper().getField(type, field).set(object, value);
    }

    public boolean existsField(String field) {
        return morphium.getConfig().getMapper().getField(type, field) != null;
    }

}
