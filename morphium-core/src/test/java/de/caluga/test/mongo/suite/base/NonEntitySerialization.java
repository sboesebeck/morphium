package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * Created by stephan on 18.11.14.
 */
@Tag("core")
public class NonEntitySerialization extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testNonEntity(Morphium morphium) throws Exception  {
        Thread.sleep(100);
        NonEntity ne = new NonEntity();
        ne.setInteger(42);
        ne.setValue("Thank you for the fish");

        Map<String, Object> obj = morphium.getMapper().serialize(ne);
        log.debug(obj.toString());

        NonEntity ne2 = morphium.getMapper().deserialize(NonEntity.class, obj);
        assert (ne2.getInteger() == 42);
        log.debug("Successful read:" + ne2);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testNonEntityList(Morphium morphium) throws Exception  {
        morphium.dropCollection(NonEntityContainer.class);
        Thread.sleep(500);
        NonEntityContainer nc = new NonEntityContainer();
        nc.setList(new ArrayList<>());
        NonEntity ne = new NonEntity();
        ne.setInteger(42);
        ne.setValue("Thank you for the fish");

        nc.getList().add(ne);
        nc.getList().add("Some string");

        Map<String, Object> obj = morphium.getMapper().serialize(nc);

        NonEntityContainer nc2 = morphium.getMapper().deserialize(NonEntityContainer.class, obj);
        assertNotNull(nc2.getList().get(0));
        ;
        NonEntity ne2 = (NonEntity) nc2.getList().get(0);
        assert (ne2.getInteger() == 42);

        //now store to Mongo
        morphium.dropCollection(NonEntityContainer.class);
        morphium.store(nc);

        Thread.sleep(1500);

        nc2 = morphium.findById(NonEntityContainer.class, nc.getId());
        assertNotNull(nc2.getList().get(0));
        ;
        ne2 = (NonEntity) nc2.getList().get(0);
        assert (ne2.getInteger() == 42);
        assert (nc2.getList().get(1).equals("Some string")) : "Wrong Value: " + nc2.getList().get(1);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testNonEntityMap(Morphium morphium) throws Exception  {
        morphium.dropCollection(NonEntityContainer.class);
        Thread.sleep(500);
        NonEntityContainer nc = new NonEntityContainer();

        nc.setMap(new HashMap<>());

        NonEntity ne = new NonEntity();
        ne.setInteger(42);
        ne.setValue("Thank you for the fish");

        nc.getMap().put("Serialized", ne);
        nc.getMap().put("String", "The question is...");


        Map<String, Object> obj = morphium.getMapper().serialize(nc);

        NonEntityContainer nc2 = morphium.getMapper().deserialize(NonEntityContainer.class, obj);
        assertNotNull(nc2.getMap().get("Serialized"));
        ;
        NonEntity ne2 = (NonEntity) nc2.getMap().get("Serialized");
        assert (ne2.getInteger() == 42);

        //now store to Mongo
        morphium.dropCollection(NonEntityContainer.class);
        morphium.store(nc);

        Thread.sleep(1500);

        nc2 = morphium.findById(NonEntityContainer.class, nc.getId());
        assertNotNull(nc2.getMap().get("Serialized"));
        ;
        ne2 = (NonEntity) nc2.getMap().get("Serialized");
        assert (ne2.getInteger() == 42);
    }


    @Entity
    public static class NonEntityContainer {
        @Id
        private MorphiumId id;
        private List<Object> list;
        private HashMap<String, Object> map;

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public List<Object> getList() {
            return list;
        }

        public void setList(List<Object> list) {
            this.list = list;
        }

        public HashMap<String, Object> getMap() {
            return map;
        }

        public void setMap(HashMap<String, Object> map) {
            this.map = map;
        }
    }


    public static class NonEntity implements Serializable {
        private String value;
        private Integer integer;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Integer getInteger() {
            return integer;
        }

        public void setInteger(Integer integer) {
            this.integer = integer;
        }

        @Override
        public String toString() {
            return "NonEntity{" +
                   "value='" + value + '\'' +
                   ", integer=" + integer +
                   '}';
        }
    }
}
