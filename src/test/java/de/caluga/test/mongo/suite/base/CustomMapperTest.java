package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.objectmapping.BsonGeoMapper;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.morphium.query.Query;
import de.caluga.morphium.query.geospatial.Geo;
import de.caluga.morphium.query.geospatial.GeoType;
import de.caluga.morphium.query.geospatial.Point;
import de.caluga.test.mongo.suite.data.CustomMappedObject;
import de.caluga.test.mongo.suite.data.CustomMappedObjectMapper;
import de.caluga.test.mongo.suite.data.ObjectWithCustomMappedObject;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 17:17
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
public class CustomMapperTest extends MorphiumTestBase {

    @Test
    public void testMongoTypes() throws Exception {
        MongoTypesTestClass cls = new MongoTypesTestClass();
        cls.stringValue = "strValue";
        cls.charValue = new Character('c');
        cls.longValue = new Long(42);
        cls.integerValue = new Integer(52);
        cls.floatValue = new Float(3.3);
        cls.doubleValue = new Double(34.2d);
        cls.dateValue = new Date();
        cls.booleanValue = new Boolean(true);
        cls.byteValue = new Byte((byte) 0x33);
        cls.shortValue = new Short((short) 12);
        cls.atomicLongValue = new AtomicLong(42);
        cls.atomicBooleanValue = new AtomicBoolean(true);
        cls.atomicIntegerValue = new AtomicInteger(42);
        cls.patternValue = Pattern.compile("a*");
        cls.bigDecimalValue = new BigDecimal(123);
        cls.uuidValue = UUID.randomUUID();
        cls.instantValue = Instant.now();
        cls.localDateValue = LocalDate.now();
        cls.localTimeValue = LocalTime.now();
        cls.localDateTimeValue = LocalDateTime.now();
        cls.timestamp = Timestamp.valueOf(cls.localDateTimeValue);
        var m = morphium.getMapper().serialize(cls);
        var o = morphium.getMapper().deserialize(MongoTypesTestClass.class, m);
        o.checkNotNull();
        morphium.store(cls);
        Thread.sleep(1000);
        FindCommand fnd = new FindCommand(morphium.getDriver().getPrimaryConnection(null));
        fnd.setDb(morphium.getDatabase()).setColl(morphium.getMapper().getCollectionName(MongoTypesTestClass.class));
        m = fnd.execute().get(0);
        var cls2 = morphium.findById(MongoTypesTestClass.class, cls.id);
        assertEquals(cls.stringValue, cls2.stringValue);
        assertEquals(cls.charValue, cls2.charValue);
        assertEquals(cls.integerValue, cls2.integerValue);
        assertEquals(cls.floatValue, cls2.floatValue);
        assertEquals(cls.doubleValue, cls2.doubleValue);
        assertEquals(cls.dateValue, cls2.dateValue);
        assertEquals(cls.booleanValue, cls2.booleanValue);
        assertEquals(cls.shortValue, cls2.shortValue);
        assertEquals(cls.byteValue, cls2.byteValue);
        assertEquals(cls.atomicLongValue.get(), cls2.atomicLongValue.get());
        assertEquals(cls.atomicBooleanValue.get(), cls2.atomicBooleanValue.get());
        assertEquals(cls.atomicIntegerValue.get(), cls2.atomicIntegerValue.get());
        assertTrue(cls.patternValue.toString().equals(cls2.patternValue.toString()));
        assertEquals(cls.bigDecimalValue, cls2.bigDecimalValue);
        assertEquals(cls.uuidValue, cls2.uuidValue);
        assertEquals(cls.instantValue, cls2.instantValue);
        assertEquals(cls.localDateValue, cls2.localDateValue);
        assertEquals(cls.localDateTimeValue, cls2.localDateTimeValue);
        assertEquals(cls.localTimeValue, cls2.localTimeValue);
    }

    @Entity()
    public static class MongoTypesTestClass {
        @Id
        public MorphiumId id;
        public String stringValue;
        public Character charValue;
        public Integer integerValue;
        public Long longValue;
        public Float floatValue;
        public Double doubleValue;
        public Date dateValue;
        public Boolean booleanValue;
        public Byte byteValue;
        public Short shortValue;
        public AtomicBoolean atomicBooleanValue;
        public AtomicInteger atomicIntegerValue;
        public AtomicLong atomicLongValue;
        public Pattern patternValue;
        public BigDecimal bigDecimalValue;
        public UUID uuidValue;
        public Instant instantValue;
        public Timestamp timestamp;
        public LocalDate localDateValue;
        public LocalTime localTimeValue;
        public LocalDateTime localDateTimeValue;

        public void checkNotNull() {
            assertNotNull(stringValue);
            assertNotNull(charValue);
            assertNotNull(integerValue);
            assertNotNull(floatValue);
            assertNotNull(doubleValue);
            assertNotNull(dateValue);
            assertNotNull(booleanValue);
            assertNotNull(shortValue);
            assertNotNull(byteValue);
            assertNotNull(atomicLongValue);
            assertNotNull(atomicBooleanValue);
            assertNotNull(atomicIntegerValue);
            assertNotNull(patternValue);
            assertNotNull(bigDecimalValue);
            assertNotNull(uuidValue);
            assertNotNull(instantValue);
            assertNotNull(localDateValue);
            assertNotNull(localDateTimeValue);
            assertNotNull(localTimeValue);
            assertNotNull(timestamp);
        }
    }

    @Test
    public void BsonGeoMapperTest() {
        BsonGeoMapper m = new BsonGeoMapper();
        Geo g = new Point(12.0, 13.0);
        Object marshalled = m.marshall(g);
        Geo res = m.unmarshall(marshalled);
        assertNotNull(res.getType());;
        assert(res.getType().equals(GeoType.POINT));
        assert(((List) res.getCoordinates()).get(0).equals(12.0));
        assert(((List) res.getCoordinates()).get(1).equals(13.0));
    }

    @Test
    public void customMappedObjectTest() {
        morphium.getMapper().registerCustomMapperFor(CustomMappedObject.class, new CustomMappedObjectMapper());
        morphium.dropCollection(ObjectWithCustomMappedObject.class);
        ObjectWithCustomMappedObject containingObject = new ObjectWithCustomMappedObject();
        List<CustomMappedObject> list = new ArrayList<>();
        Map<String, CustomMappedObject> map = new HashMap<>();
        mockupContainerObject(containingObject, list, map);
        morphium.store(containingObject);
        Query<ObjectWithCustomMappedObject> q = morphium.createQueryFor(ObjectWithCustomMappedObject.class).f("id").eq(containingObject.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        ObjectWithCustomMappedObject readContainingObject = q.get();
        assertNotNull(readContainingObject, "Error - not found?");
        assertNotNull(readContainingObject.getCustomMappedObject(), "Custom mapped object null?");
        assertNotNull(readContainingObject.getCustomMappedObjectList(), "List of custom mapped object null?");
        assertNotNull(readContainingObject.getCustomMappedObjectMap(), "Map with custom mapped object null?");
        assert(readContainingObject.getCustomMappedObjectList().size() == 2) : "List of custom mapped objects has wrong size? size is " + readContainingObject.getCustomMappedObjectList().size();
        assert(readContainingObject.getCustomMappedObjectMap().size() == 2) : "Map with custom mapped objects as value has wrong size?";
        assert(readContainingObject.getCustomMappedObject().equals(containingObject.getCustomMappedObject())) : "Single custom mapped objects differ?";

        for (int i = 0; i < 2; i++) {
            CustomMappedObject referenceObject = containingObject.getCustomMappedObjectList().get(i);
            assertNotNull(readContainingObject.getCustomMappedObjectList().get(i), "Custom mapped object in list missing? - " + i);
            assert(readContainingObject.getCustomMappedObjectList().get(i).equals(referenceObject)) : "Custom mapped objects in list differ? - " + i;
            assert(readContainingObject.getCustomMappedObjectMap().get(referenceObject.getName()).equals(map.get(referenceObject.getName()))) : "Custom mapped objects in map differ? - " + i;
        }

        morphium.getMapper().deregisterCustomMapperFor(CustomMappedObject.class);
    }

    private void mockupContainerObject(ObjectWithCustomMappedObject containingObject, List<CustomMappedObject> list, Map<String, CustomMappedObject> map) {
        CustomMappedObject singleCustomMappedObject = new CustomMappedObject();
        singleCustomMappedObject.setName("customMapped");
        singleCustomMappedObject.setValue("single");
        singleCustomMappedObject.setIntValue(-1);
        int count = 2;

        for (int i = 0; i < count; i++) {
            CustomMappedObject customMappedObject = new CustomMappedObject();
            customMappedObject.setName("customMappedObject#" + i);
            customMappedObject.setValue("number: " + i);
            customMappedObject.setIntValue(i);
            list.add(customMappedObject);
            map.put(customMappedObject.getName(), customMappedObject);
        }

        containingObject.setCustomMappedObject(singleCustomMappedObject);
        containingObject.setCustomMappedObjectList(list);
        containingObject.setCustomMappedObjectMap(map);
    }

    @Test
    public void customMapperObjectIdTest() {
        MorphiumTypeMapper<ObjectIdTest> mapper = new MorphiumTypeMapper<ObjectIdTest>() {
            @Override
            public Object marshall(ObjectIdTest o) {
                Map serialized = new HashMap();
                serialized.put("value", o.value);
                serialized.put("_id", o.id);
                return serialized;
            }

            @Override
            public ObjectIdTest unmarshall(Object d) {
                Map obj = ((Map) d);
                ObjectIdTest o = new ObjectIdTest();
                o.id = new ObjectId(obj.get("_id").toString());
                o.value = (String)(obj.get("value"));
                return o;
            }
        };

        morphium.getMapper().registerCustomMapperFor(ObjectIdTest.class, mapper);
        ObjectIdTest t = new ObjectIdTest();
        t.value = "test1";
        t.id = new ObjectId();
        morphium.store(t);
        morphium.reread(t);
        t = new ObjectIdTest();
        t.value = "test2";
        t.id = new ObjectId();
        morphium.store(t);
        List<ObjectIdTest> lst = morphium.createQueryFor(ObjectIdTest.class).asList();

        for (ObjectIdTest tst : lst) {
            log.info("T: " + tst.value + " id: " + tst.id.toHexString());
        }

        morphium.getMapper().deregisterCustomMapperFor(ObjectIdTest.class);
    }

    @Test
    public void complexCustomMappingTest() {
        morphium.getMapper().registerCustomMapperFor(CustomMappedObject.class, new CustomMappedObjectMapper());
        morphium.dropCollection(ObjectWithCustomMappedObject.class);
        ComplexCustomMapperObject containingObject = new ComplexCustomMapperObject();
        List<Map<String, List<CustomMappedObject >>> complexestList = new ArrayList<>();
        Map<String, List<Map<String, CustomMappedObject >>> complexestMap = new HashMap<>();
        List<Map<String, CustomMappedObject >> complexList = new ArrayList<>();
        Map<String, List<CustomMappedObject >> complexMap = new HashMap<>();
        List<CustomMappedObject> list = new ArrayList<>();
        Map<String, CustomMappedObject> map = new HashMap<>();
        mockupContainerObject(containingObject, list, map);
        complexList.add(map);
        complexMap.put("a_list", list);
        complexestList.add(complexMap);
        complexestMap.put("a_complex_list", complexList);
        containingObject.setComplexList(complexList);
        containingObject.setComplexMap(complexMap);
        containingObject.setComplexestList(complexestList);
        containingObject.setComplexestMap(complexestMap);
        morphium.store(containingObject);
        Query<ComplexCustomMapperObject> q = morphium.createQueryFor(ComplexCustomMapperObject.class).f("id").eq(containingObject.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        ComplexCustomMapperObject readContainingObject = q.get();
        assertNotNull(readContainingObject, "Error - not found?");
        assertNotNull(readContainingObject.getComplexList(), "Complex list object null?");
        assertNotNull(readContainingObject.getComplexMap(), "Complex map object null?");
        assertNotNull(readContainingObject.getComplexestList(), "Complexest list object null?");
        assertNotNull(readContainingObject.getComplexestMap(), "Complexest map object null?");
        assert(readContainingObject.getComplexList().size() == 1) : "Complex list has wrong size?";
        assert(readContainingObject.getComplexMap().size() == 1) : "Complex map has wrong size?";
        assert(readContainingObject.getComplexestList().size() == 1) : "Complexest list has wrong size?";
        assert(readContainingObject.getComplexestMap().size() == 1) : "Complexest map has wrong size?";
        assert(readContainingObject.getComplexList().equals(complexList)) : "Complex lists differ?";
        assert(readContainingObject.getComplexMap().equals(complexMap)) : "Complex maps differ?";
        assert(readContainingObject.getComplexestList().equals(complexestList)) : "Complexest lists differ?";
        assert(readContainingObject.getComplexestMap().equals(complexestMap)) : "Complexest maps differ?";
        morphium.getMapper().deregisterCustomMapperFor(CustomMappedObject.class);
    }

    private static class ComplexCustomMapperObject extends ObjectWithCustomMappedObject {
        private List<Map<String, CustomMappedObject >> complexList;
        private Map<String, List<CustomMappedObject >> complexMap;

        private List<Map<String, List<CustomMappedObject >>> complexestList;
        private Map<String, List<Map<String, CustomMappedObject >>> complexestMap;

        public List<Map<String, CustomMappedObject >> getComplexList() {
            return complexList;
        }

        public void setComplexList(List<Map<String, CustomMappedObject >> complexList) {
            this.complexList = complexList;
        }

        public Map<String, List<CustomMappedObject >> getComplexMap() {
            return complexMap;
        }

        public void setComplexMap(Map<String, List<CustomMappedObject >> complexMap) {
            this.complexMap = complexMap;
        }

        public List<Map<String, List<CustomMappedObject >>> getComplexestList() {
            return complexestList;
        }

        public void setComplexestList(List<Map<String, List<CustomMappedObject >>> complexestList) {
            this.complexestList = complexestList;
        }

        public Map<String, List<Map<String, CustomMappedObject >>> getComplexestMap() {
            return complexestMap;
        }

        public void setComplexestMap(Map<String, List<Map<String, CustomMappedObject >>> complexestMap) {
            this.complexestMap = complexestMap;
        }
    }

    @Entity
    public static class ObjectIdTest {
        @Id
        public ObjectId id;
        public String value;

        public enum Fields {
            value, id
        }
    }

}
