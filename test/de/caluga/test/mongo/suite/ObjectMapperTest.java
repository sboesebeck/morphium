package de.caluga.test.mongo.suite;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.ObjectMapperImpl;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.*;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 14:04
 * <p/>
 * TODO: Add documentation here
 */
public class ObjectMapperTest extends MongoTest {
    @Test
    public void testCreateCamelCase() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        assert (om.createCamelCase("this_is_a_test", false).equals("thisIsATest")) : "Error camil case translation not working";
        assert (om.createCamelCase("a_test_this_is", true).equals("ATestThisIs")) : "Error - capitalized String wrong";


    }

    @Test
    public void testConvertCamelCase() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        assert (om.convertCamelCase("thisIsATest").equals("this_is_a_test")) : "Conversion failed!";
    }

    @Test
    public void testGetCollectionName() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        assert (om.getCollectionName(CachedObject.class).equals("cached_object")) : "Cached object test failed";
        assert (om.getCollectionName(UncachedObject.class).equals("uncached_object")) : "Uncached object test failed";

    }

    @Test
    public void testMarshall() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        DBObject dbo = om.marshall(o);
        System.out.println("Marshalling was: " + dbo.toString());
        assert (dbo.toString().equals("{ \"value\" : \"This \\\" is $ test\" , \"counter\" : 12345}")) : "String creation failed?" + dbo.toString();
    }

    @Test
    public void testUnmarshall() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        BasicDBObject dbo = new BasicDBObject();
        dbo.put("counter", 12345);
        dbo.put("value", "A test");
        om.unmarshall(UncachedObject.class, dbo);
    }

    @Test
    public void testGetId() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        o.setMongoId(new ObjectId(new Date()));
        ObjectId id = om.getId(o);
        assert (id.equals(o.getMongoId())) : "IDs not equal!";
    }


    @Test
    public void testIsEntity() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        assert (om.isEntity(UncachedObject.class)) : "Uncached Object no Entity?=!?=!?";
        assert (om.isEntity(new UncachedObject())) : "Uncached Object no Entity?=!?=!?";
        assert (!om.isEntity("")) : "String is an Entity?";
    }

    @Test
    public void testGetValue() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(null);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        assert (om.getValue(o, "counter").equals(12345)) : "Value not ok!";

    }

    @Test
    public void testSetValue() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl(null);
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        om.setValue(o, "A test", "value");
        assert ("A test".equals(o.getValue())) : "Value not set";

    }


    @Test
    public void complexObjectTest() {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("Embedded value");
        MorphiumSingleton.get().store(o);

        EmbeddedObject eo = new EmbeddedObject();
        eo.setName("Embedded only");
        eo.setValue("Value");
        eo.setTest(System.currentTimeMillis());

        ComplexObject co = new ComplexObject();
        co.setEinText("Ein text");
        co.setEmbed(eo);

        co.setEntityEmbeded(o);
        ObjectId embedId = o.getMongoId();

        o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("Referenced value");
//        o.setMongoId(new ObjectId(new Date()));
        MorphiumSingleton.get().store(o);

        co.setRef(o);
        co.setId(new ObjectId(new Date()));
        String st = co.toString();
        System.out.println("Referenced object: " + om.marshall(o).toString());
        DBObject marshall = om.marshall(co);
        System.out.println("Complex object: " + marshall.toString());


        //Unmarshalling stuff
        co = om.unmarshall(ComplexObject.class, marshall);
        assert (co.getEntityEmbeded().getMongoId() == null) : "Embeded entity got a mongoID?!?!?!";
        co.getEntityEmbeded().setMongoId(embedId);  //need to set ID manually, as it won't be stored!
        String st2 = co.toString();
        assert (st.equals(st2)) : "Strings not equal?\n" + st + "\n" + st2;
        assert (co.getEmbed() != null) : "Embedded value not found!";

    }

    @Test
    public void nullValueTests() {
        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());

        ComplexObject o = new ComplexObject();
        o.setTrans("TRANSIENT");
        DBObject obj = null;
        try {
            obj = om.marshall(o);
        } catch (IllegalArgumentException e) {
        }
        o.setEinText("Ein Text");
        obj = om.marshall(o);
        assert (!obj.containsField("trans")) : "Transient field used?!?!?";
    }

    @Test
    public void listValueTest() {
        MapListObject o = new MapListObject();
        List lst = new ArrayList();
        lst.add("A Value");
        lst.add(27.0);
        lst.add(new UncachedObject());

        o.setListValue(lst);
        o.setName("Simple List");

        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        DBObject marshall = om.marshall(o);
        String m = marshall.toString();

        assert (m.equals("{ \"name\" : \"Simple List\" , \"list_value\" : [ \"A Value\" , 27.0 , { \"counter\" : 0 , \"class_name\" : \"de.caluga.test.mongo.suite.UncachedObject\"}]}")) : "Marshall not ok: " + m;

        MapListObject mo = om.unmarshall(MapListObject.class, marshall);
        System.out.println("Mo: " + mo.getName());
        System.out.println("lst: " + mo.getListValue());
        assert (mo.getName().equals(o.getName())) : "Names not equal?!?!?";
        for (int i = 0; i < lst.size(); i++) {
            Object listValueNew = mo.getListValue().get(i);
            Object listValueOrig = o.getListValue().get(i);
            assert (listValueNew.getClass().equals(listValueOrig.getClass())) : "Classes differ: " + listValueNew.getClass() + " - " + listValueOrig.getClass();
            assert (listValueNew.equals(listValueOrig)) : "Value not equals in list: " + listValueNew + " vs. " + listValueOrig;
        }
        System.out.println("test Passed!");

    }


    @Test
    public void mapValueTest() {
        MapListObject o = new MapListObject();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("a_string", "This is a string");
        map.put("a primitive value", 42);
        map.put("double", 42.0);
        map.put("null", null);
        map.put("Entity", new UncachedObject());
        o.setMapValue(map);
        o.setName("A map-value");

        ObjectMapperImpl om = new ObjectMapperImpl(MorphiumSingleton.get());
        DBObject marshall = om.marshall(o);
        String m = marshall.toString();
        System.out.println("Marshalled object: " + m);
        assert (m.equals("{ \"name\" : \"A map-value\" , \"map_value\" : { \"a_string\" : \"This is a string\" , \"a primitive value\" : 42 , \"double\" : 42.0 , \"null\" :  null  , \"Entity\" : { \"counter\" : 0 , \"class_name\" : \"de.caluga.test.mongo.suite.UncachedObject\"}}}")) : "Value not marshalled coorectly";

        MapListObject mo = om.unmarshall(MapListObject.class, marshall);
        assert (mo.getName().equals("A map-value")) : "Name error";
        assert (mo.getMapValue() != null) : "map value is null????";
        for (String k : mo.getMapValue().keySet()) {
            Object v = mo.getMapValue().get(k);
            if (v == null) {
                assert (o.getMapValue().get(k) == null) : "v==null but original not?";
            } else {
                assert (o.getMapValue().get(k).getClass().equals(v.getClass())) : "Classes differ: " + o.getMapValue().get(k).getClass().getName() + " != " + v.getClass().getName();
                assert (o.getMapValue().get(k).equals(v)) : "Value not equal, key: " + k;
            }
        }

    }
}
