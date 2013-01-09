package de.caluga.morphium;

import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:24
 * <p/>
 * Maps objects to Mongo
 */
public interface ObjectMapper {

    public String getCollectionName(Class cls);

    public String createCamelCase(String n, boolean capitalize);

    public String convertCamelCase(String n);

    public DBObject marshall(Object o);

    public <T> T unmarshall(Class<? extends T> cls, DBObject o);

    public ObjectId getId(Object o);

    /**
     * de-Referencing class - handling for Lazy-Dereferencing und parital update
     *
     * @param o - the object to check (might be a proxy)=
     * @return - the real object
     */
    public <T> T getRealObject(T o);

    /**
     * de-Referencing class - handling for Lazy-Dereferencing und parital update
     *
     * @param o - class to look for
     * @return real class
     */
    public <T> Class<? extends T> getRealClass(Class<? extends T> o);

    public List<String> getFields(Class o, Class<? extends Annotation>... annotations);

    public Field getField(Class cls, String fld);

    public String getFieldName(Class cls, String field);

    public boolean isEntity(Object o);

    public Object getValue(Object o, String fld);

    public void setValue(Object o, Object value, String fld);

    public Morphium getMorphium();

    public void setMorphium(Morphium m);

    public List<Field> getAllFields(Class cls);

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
