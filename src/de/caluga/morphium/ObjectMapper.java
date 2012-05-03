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
 * TODO: Add documentation here
 */
public interface ObjectMapper {

    public String getCollectionName(Class cls);

    public String createCamelCase(String n, boolean capitalize);

    public String convertCamelCase(String n);

    public DBObject marshall(Object o);

    public <T> T unmarshall(Class<T> cls, DBObject o);

    public ObjectId getId(Object o);

    public List<String> getFields(Class o, Class<? extends Annotation>... annotations);

    public Field getField(Class cls, String fld);

    public boolean isEntity(Object o);

    public Object getValue(Object o, String fld);

    public void setValue(Object o, Object value, String fld);

    public Morphium getMorphium();
    public void setMorphium(Morphium m);
}
