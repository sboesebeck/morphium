package de.caluga.morphium;

import com.mongodb.DBObject;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:24
 * <p/>
 * Maps objects to Mongo
 */
public interface ObjectMapper {

    public String getCollectionName(Class cls);

    public DBObject marshall(Object o);

    public <T> T unmarshall(Class<? extends T> cls, DBObject o);

    /**
     * get current name provider for class
     *
     * @param cls - class
     * @return configured name provider in @Entity or currently set one
     */
    @SuppressWarnings("UnusedDeclaration")
    public NameProvider getNameProviderForClass(Class<?> cls);

    /**
     * override settings vor name Provider from @Entity
     *
     * @param cls - class
     * @param np  the name Provider to use
     */
    public void setNameProviderForClass(Class<?> cls, NameProvider np);
}
