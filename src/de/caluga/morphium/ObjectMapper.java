package de.caluga.morphium;

import com.mongodb.DBObject;
import org.json.simple.parser.ParseException;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:24
 * <p/>
 * Maps objects to Mongo
 */
public interface ObjectMapper {

    String getCollectionName(Class cls);

    Object marshallIfNecessary(Object o);

    DBObject marshall(Object o);

    <T> T unmarshall(Class<? extends T> cls, DBObject o);

    <T> T unmarshall(Class<? extends T> cls, String json) throws ParseException;

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

    void setMorphium(Morphium m);

    Morphium getMorphium();


}
