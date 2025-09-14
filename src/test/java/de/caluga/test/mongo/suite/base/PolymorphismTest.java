package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.NameProvider;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 27.11.13
 * Time: 08:39
 * Test polymorphism mechanism in Morphium
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
public class PolymorphismTest extends MorphiumTestBase {
    @Test
    public void polymorphTest() throws Exception {
        morphium.dropCollection(PolyTest.class);
        OtherSubClass p = new OtherSubClass();
        p.setPoly("poly");
        p.setOther("other");
        morphium.store(p);

        SubClass sb = new SubClass();
        sb.setPoly("poly super");
        sb.setSub("sub");
        morphium.store(sb);
        Thread.sleep(100);
        assert (morphium.createQueryFor(PolyTest.class).countAll() == 2);
        List<PolyTest> lst = morphium.createQueryFor(PolyTest.class).asList();
        for (PolyTest tst : lst) {
            log.info("Class " + tst.getClass().toString());
        }
    }

    public static class PolyNameProvider implements NameProvider {

        @Override
        public String getCollectionName(Class<?> type, MorphiumObjectMapper om, boolean translateCamelCase, boolean useFQN, String specifiedName, Morphium morphium) {
            return "poly";
        }
    }

    @Test
    public void subClasstest() throws Exception {
        PolyContainer pc = new PolyContainer();
        pc.aSubClass = new SubClass();
        ((SubClass) pc.aSubClass).setSub("Subclass");

        Map<String, Object> obj = morphium.getMapper().serialize(pc);
        assertNotNull(obj);
        ;

        pc = morphium.getMapper().deserialize(PolyContainer.class, obj);
        assert (pc.aSubClass instanceof SubClass);

        pc = new PolyContainer();
        pc.aLotOfSubClasses = new ArrayList<>();
        pc.aLotOfSubClasses.add(new SubClass("subclass"));
        pc.aLotOfSubClasses.add(new OtherSubClass("othersubclass"));
        pc.aLotOfSubClasses.add(new SubClass("subclass"));

        obj = morphium.getMapper().serialize(pc);
        assertNotNull(obj);
        ;
        assert (((List) obj.get("a_lot_of_sub_classes")).size() == 3);

        pc = morphium.getMapper().deserialize(PolyContainer.class, obj);
        assertNotNull(pc);
        ;
        assert (pc.aLotOfSubClasses.size() == 3);

        pc = new PolyContainer();
        pc.aMapOfSubClasses = new HashMap<>();
        pc.aMapOfSubClasses.put("subClass", new SubClass("subclass"));
        pc.aMapOfSubClasses.put("other", new OtherSubClass("other"));
        obj = morphium.getMapper().serialize(pc);
        assertNotNull(obj);
        ;

        pc = morphium.getMapper().deserialize(PolyContainer.class, obj);
        assertNotNull(pc.aMapOfSubClasses.get("subClass"));
        ;
        assertNotNull(pc.aMapOfSubClasses.get("other"));
        ;
    }

    @Entity
    public static class PolyContainer {
        @Id
        public MorphiumId id;
        public PolyTest aSubClass;
        public List<PolyTest> aLotOfSubClasses;
        public Map<String, PolyTest> aMapOfSubClasses;
    }

    @Entity(polymorph = true, nameProvider = PolyNameProvider.class)
    @NoCache
    public static abstract class PolyTest {
        private String poly;

        @Id
        private MorphiumId id;

        public String getPoly() {
            return poly;
        }

        public void setPoly(String poly) {
            this.poly = poly;
        }
    }

    public static class SubClass extends PolyTest {
        private String sub;

        public SubClass() {
        }

        public SubClass(String sub) {
            this.sub = sub;
        }

        public String getSub() {
            return sub;
        }

        public void setSub(String sub) {
            this.sub = sub;
        }
    }

    public static class OtherSubClass extends PolyTest {
        private String other;

        public OtherSubClass() {
        }

        public OtherSubClass(String other) {
            this.other = other;
        }

        public String getOther() {
            return other;
        }

        public void setOther(String o) {
            this.other = o;
        }
    }
}
