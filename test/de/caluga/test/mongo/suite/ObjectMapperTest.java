package de.caluga.test.mongo.suite;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.ObjectMapperImpl;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Date;

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
        ObjectMapperImpl om = new ObjectMapperImpl();
        assert (om.createCamelCase("this_is_a_test", false).equals("thisIsATest")) : "Error camil case translation not working";
        assert (om.createCamelCase("a_test_this_is", true).equals("ATestThisIs")) : "Error - capitalized String wrong";


    }

    @Test
    public void testConvertCamelCase() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        assert (om.convertCamelCase("thisIsATest").equals("this_is_a_test")) : "Conversion failed!";
    }

    @Test
    public void testGetCollectionName() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        assert (om.getCollectionName(CachedObject.class).equals("cached_object")) : "Cached object test failed";
        assert (om.getCollectionName(UncachedObject.class).equals("uncached_object")) : "Uncached object test failed";

    }

    @Test
    public void testMarshall() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        DBObject dbo = om.marshall(o);
        System.out.println("Marshalling was: " + dbo.toString());
        assert (dbo.toString().equals("{ \"value\" : \"This \\\" is $ test\" , \"counter\" : 12345 , \"_id\" :  null }")) : "String creation failed?" + dbo.toString();
    }

    @Test
    public void testUnmarshall() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        BasicDBObject dbo = new BasicDBObject();
        dbo.put("counter", 12345);
        dbo.put("value", "A test");
        om.unmarshall(UncachedObject.class, dbo);
    }

    @Test
    public void testGetId() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        o.setMongoId(new ObjectId(new Date()));
        ObjectId id = om.getId(o);
        assert (id.equals(o.getMongoId())) : "IDs not equal!";
    }


    @Test
    public void testIsEntity() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        assert (om.isEntity(UncachedObject.class)) : "Uncached Object no Entity?=!?=!?";
        assert (om.isEntity(new UncachedObject())) : "Uncached Object no Entity?=!?=!?";
        assert (!om.isEntity("")) : "String is an Entity?";
    }

    @Test
    public void testGetValue() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("This \" is $ test");
        assert (om.getValue(o, "counter").equals(12345)) : "Value not ok!";

    }

    @Test
    public void testSetValue() throws Exception {
        ObjectMapperImpl om = new ObjectMapperImpl();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        om.setValue(o, "A test", "value");
        assert ("A test".equals(o.getValue())) : "Value not set";

    }


    @Test
    public void complexObjectTest() {
        ObjectMapperImpl om = new ObjectMapperImpl();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("Embedded value");
        Morphium.get().store(o);

        ComplexObject co = new ComplexObject();
        co.setEinText("Ein text");
        co.setEmbed(o);
        ObjectId embedId = o.getMongoId();

        o = new UncachedObject();
        o.setCounter(12345);
        o.setValue("Referenced value");
//        o.setMongoId(new ObjectId(new Date()));
        Morphium.get().store(o);

        co.setRef(o);
        co.setId(new ObjectId(new Date()));
        String st = co.toString();
        System.out.println("Referenced object: " + om.marshall(o).toString());
        DBObject marshall = om.marshall(co);
        System.out.println("Complex object: " + marshall.toString());

        //Unmarshalling stuff


        co = om.unmarshall(ComplexObject.class, marshall);
        co.getEmbed().setMongoId(embedId);  //need to set ID manually, as it won't be stored!
        String st2 = co.toString();
        assert (st.equals(st2)) : "Strings not equal?\n" + st + "\n" + st2;

    }
}
