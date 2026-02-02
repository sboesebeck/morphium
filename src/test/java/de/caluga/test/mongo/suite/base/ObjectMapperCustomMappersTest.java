package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumReference;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class ObjectMapperCustomMappersTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void customTypeMapperTest(Morphium morphium) throws InterruptedException {
        morphium.dropCollection(BIObject.class);
        MorphiumObjectMapper om = morphium.getMapper();
        BigInteger tst = new BigInteger("affedeadbeefaffedeadbeef42", 16);
        Map<String, Object> d = om.serialize(tst);

        BigInteger bi = om.deserialize(BigInteger.class, d);
        assertNotNull(bi);
        ;
        assertEquals(tst, bi);

        BIObject bio = new BIObject();
        bio.biValue = tst;
        morphium.store(bio);
        TestUtils.waitForWrites(morphium, log);
        TestUtils.waitForConditionToBecomeTrue(5000, "BIObject not found",
            () -> morphium.createQueryFor(BIObject.class).countAll() > 0);

        BIObject bio2 = morphium.createQueryFor(BIObject.class).get();
        assertNotNull(bio2);
        assertNotNull(bio2.biValue);
        assertEquals(bio2.biValue, tst);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void objectMapperNGTest(Morphium morphium) {
        MorphiumObjectMapper map = morphium.getMapper();
        UncachedObject uc = new UncachedObject("value", 123);
        uc.setMorphiumId(new MorphiumId());
        uc.setLongData(new long[] {1L, 2L});
        Map<String, Object> obj = map.serialize(uc);

        assertNotNull(obj.get("str_value"));
        ;
        assertTrue(obj.get("str_value") instanceof String);
        assertTrue(obj.get("counter") instanceof Integer);
        assertTrue(obj.get("long_data") instanceof java.util.ArrayList);

        MappedObject mo = new MappedObject();
        mo.id = "test";
        mo.uc = uc;
        mo.aMap = new HashMap<>();
        mo.aMap.put("Test", "value1");
        mo.aMap.put("test2", "value2");
        obj = map.serialize(mo);
        assertNotNull(obj.get("uc"));
        ;
        assertTrue(((Map) obj.get("uc")).get("_id") == null);

        BIObject bo = new BIObject();
        bo.id = new MorphiumId();
        bo.value = "biVal";
        bo.biValue = new BigInteger("123afd33", 16);

        obj = map.serialize(bo);
        assertTrue(obj.get("_id") instanceof ObjectId || obj.get("_id") instanceof String || obj.get("_id") instanceof MorphiumId);
        assertInstanceOf(Map.class, obj.get("bi_value"));
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void enumTest(Morphium morphium) {
        ObjectMapperEnumTestContainer e = new ObjectMapperEnumTestContainer();
        e.anEnum = TestEnum.v1;
        e.aMap = new HashMap<>();
        e.aMap.put("test1", TestEnum.v2);
        e.aMap.put("test3", TestEnum.v3);

        e.lst = new java.util.ArrayList<>();
        e.lst.add(TestEnum.v4);
        e.lst.add(TestEnum.v3);
        e.lst.add(TestEnum.v1);


        Map<String, Object> obj = morphium.getMapper().serialize(e);
        assertNotNull(obj.get("an_enum"));
        ;

        MorphiumObjectMapper map = new ObjectMapperImpl();
        map.setMorphium(morphium);
        map.setAnnotationHelper(morphium.getARHelper());
        Map<String, Object> obj2 = map.serialize(e);
        assertNotNull(obj2.get("an_enum"));
        ;

        ObjectMapperEnumTestContainer e2 = map.deserialize(ObjectMapperEnumTestContainer.class, obj2);

        assertNotNull(e2);
        ;
        assertEquals(e2, e);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void referenceTest(Morphium morphium) {
        MorphiumReference r = new MorphiumReference("test", new MorphiumId());
        Map<String, Object> o = morphium.getMapper().serialize(r);
        assertNotNull(o.get("refid"));
        ;

        MorphiumReference r2 = morphium.getMapper().deserialize(MorphiumReference.class, o);
        assertNotNull(r2.getId());
        ;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void customMapperTest(Morphium morphium) {
        morphium.getMapper().registerCustomMapperFor(MyClass.class, new MorphiumTypeMapper<MyClass>() {
            @Override
            public Object marshall(MyClass o) {
                Map m = new HashMap();
                m.put("class", o.getClass().getName());
                m.put("value", "AMMENDED+" + o.theValue);
                return m;
            }

            @Override
            public MyClass unmarshall(Object d) {
                Map m = (Map) d;
                MyClass mc = new MyClass();
                mc.theValue = (String) m.get("value");
                mc.theValue = mc.theValue.substring(9);

                return mc;
            }
        });

        MyClass mc = new MyClass();
        mc.theValue = "a little Test";
        Map<String, Object> map = morphium.getMapper().serialize(mc);
        assertEquals(map.get("class"), mc.getClass().getName());
        assertEquals(map.get("value"), "AMMENDED+" + mc.theValue);

        MyClass mc2 = morphium.getMapper().deserialize(MyClass.class, map);
        assertEquals(mc2.theValue, mc.theValue);

    }
}
