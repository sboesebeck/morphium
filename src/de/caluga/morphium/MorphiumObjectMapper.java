package de.caluga.morphium;

import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.Map;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:24
 * <p/>
 * Maps objects to Mongo
 */
public interface MorphiumObjectMapper {

    String getCollectionName(Class cls);

    Object marshallIfNecessary(Object o);

    Map<String, Object> serialize(Object o);

    <T> T deserialize(Class<? extends T> cls, Map<String, Object> o);

    <T> T deserialize(Class<? extends T> cls, String json) throws ParseException, IOException;

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

    void setAnnotationHelper(AnnotationAndReflectionHelper an);

    Morphium getMorphium();

    void setMorphium(Morphium m);


}
