package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.replicaset.ReplicaSetConf;
import de.caluga.test.mongo.suite.data.ComplexObject;
import de.caluga.test.mongo.suite.data.EmbeddedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class ObjectMapperComplexObjectTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void complexObjectTest(Morphium morphium) {
        MorphiumObjectMapper om = morphium.getMapper();
        UncachedObject o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("Embedded value");
        morphium.store(o);

        EmbeddedObject eo = new EmbeddedObject();
        eo.setName("Embedded only");
        eo.setValue("Value");
        eo.setTest(System.currentTimeMillis());

        ComplexObject co = new ComplexObject();
        co.setEinText("Ein text");
        co.setEmbed(eo);

        co.setEntityEmbeded(o);
        MorphiumId embedId = o.getMorphiumId();

        o = new UncachedObject();
        o.setCounter(12345);
        o.setStrValue("Referenced value");
        morphium.store(o);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }

        co.setRef(o);
        co.setId(new MorphiumId());
        String st = Utils.toJsonString(co);
        System.out.println("Referenced object: " + Utils.toJsonString(om.serialize(o)));
        Map<String, Object> marshall = om.serialize(co);
        System.out.println("Complex object: " + Utils.toJsonString(marshall));


        //Unmarshalling stuff
        co = om.deserialize(ComplexObject.class, marshall);
        assertNull(co.getEntityEmbeded().getMorphiumId());
        assertNotNull(co.getRef());
        ;
        co.getEntityEmbeded().setMorphiumId(embedId);  //need to set ID manually, as it won't be stored!
        co.getRef().setMorphiumId(o.getMorphiumId());
        String st2 = Utils.toJsonString(co);
        assertTrue(stringWordCompare(st, st2));
        assertNotNull(co.getEmbed(), "Embedded value not found!");

    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void nullValueTests(Morphium morphium) {
        MorphiumObjectMapper om = morphium.getMapper();

        ComplexObject o = new ComplexObject();
        o.setTrans("TRANSIENT");
        Map<String, Object> obj;
        try {
            obj = om.serialize(o);
        } catch (IllegalArgumentException e) {
        }
        o.setEinText("Ein Text");
        obj = om.serialize(o);
        assertFalse(obj.containsKey("trans"));
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void rsStatusTest(Morphium morphium) throws Exception {
        morphium.getConfig().setReplicasetMonitoring(false);
        String json = "{ \"settings\" : { \"heartbeatTimeoutSecs\" : 10, \"catchUpTimeoutMillis\" : -1, \"catchUpTakeoverDelayMillis\" : 30000, \"getLastErrorModes\" : {  } , \"getLastErrorDefaults\" : { \"wtimeout\" : 0, \"w\" : 1 } , \"electionTimeoutMillis\" : 10000, \"chainingAllowed\" : true, \"replicaSetId\" : \"5adba61c986af770bb25454e\", \"heartbeatIntervalMillis\" : 2000 } , \"members\" :  [ { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : false, \"host\" : \"localhost:27017\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 0, \"priority\" : 10.0, \"tags\" : {  }  } , { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : false, \"host\" : \"localhost:27018\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 1, \"priority\" : 5.0, \"tags\" : {  }  } , { \"hidden\" : false, \"buildIndexes\" : true, \"arbiterOnly\" : true, \"host\" : \"localhost:27019\", \"slaveDelay\" : 0, \"votes\" : 1, \"_id\" : 2, \"priority\" : 0.0, \"tags\" : {  }  } ], \"protocolVersion\" : 1, \"_id\" : \"tst\", \"version\" : 1 } ";
        ReplicaSetConf c = morphium.getMapper().deserialize(ReplicaSetConf.class, json);
        assertNotNull(c);
        ;
        assertEquals(3, c.getMembers().size());
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void mapTest(Morphium morphium) throws Exception {
        MorphiumObjectMapper m = morphium.getMapper();

        MappedObject o = new MappedObject();
        o.aMap = new HashMap<>();
        o.aMap.put("test", "test");
        o.uc = new NoDefaultConstructorUncachedObject("v", 123);

        Map<String, Object> dbo = m.serialize(o);
        o = m.deserialize(MappedObject.class, Utils.toJsonString(dbo));

        assertNotNull(o.aMap.get("test"));
        ;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testStructure(Morphium morphium) throws Exception {
        Complex c = new Complex();
        c.id = new MorphiumId();
        c.structureK = new ArrayList<>();

        Map<String, Object> v1 = new HashMap<>();
        v1.put("String", "String");
        v1.put("Integer", 123);
        v1.put("List", Arrays.asList("l1", "l2"));
        c.structureK.add(v1);

        Map<String, Object> v2 = new HashMap<>();
        v2.put("String", "String");
        v2.put("Integer", 123);
        v2.put("List", Arrays.asList("l1", "l2"));
        v2.put("Map", UtilsMap.of("key", 123));
        c.structureK.add(v2);

        Map<String, Object> seralized = new ObjectMapperImpl().serialize(c);
        log.info(Utils.toJsonString(seralized));

        Complex c2 = new ObjectMapperImpl().deserialize(Complex.class, seralized);
        log.info("Deserialized!");
        assertNotNull(c2);
        ;
        assertEquals(c2.id, c.id);
        assertEquals(c2.structureK.size(), c.structureK.size());
        assertInstanceOf(String.class, c2.structureK.get(0).get("String"));
        assertInstanceOf(Integer.class, c2.structureK.get(0).get("Integer"));
        assertInstanceOf(List.class, c2.structureK.get(0).get("List"));
        assertNull(c2.structureK.get(0).get("Map"));
        assertInstanceOf(String.class, c2.structureK.get(1).get("String"));
        assertInstanceOf(Integer.class, c2.structureK.get(1).get("Integer"));
        assertInstanceOf(List.class, c2.structureK.get(1).get("List"));
        assertNotNull(c2.structureK.get(1).get("Map"));
        ;
        assertEquals(123, ((Map) c2.structureK.get(1).get("Map")).get("key"));

        log.info("All fine!");
    }
}
