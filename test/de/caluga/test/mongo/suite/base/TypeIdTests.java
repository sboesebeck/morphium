package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.MorphiumReference;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TypeIdTests extends MorphiumTestBase {

    @Test
    public void testAdditionalDataEmbedded() throws Exception {
        AdditionalDataEntity ad = new AdditionalDataEntity();
        ad.setStrValue("test");
        ad.setCounter(12);
        ad.setAdditionals(UtilsMap.of("test", new EmbeddedObject("name", "value", 123)));
        morphium.store(ad);

        Thread.sleep(100);
        ad.setAdditionals(null);
        morphium.reread(ad);
        assert (ad.getAdditionals() != null);
        assert (ad.getAdditionals().containsKey("test"));
        assert (ad.getAdditionals().get("test") instanceof EmbeddedObject);
        assert (((EmbeddedObject) ad.getAdditionals().get("test")).getName().equals("name"));
        checkTypeId(EmbeddedObject.class, ad, "test");

    }


    @Test
    public void testAdditionalDataEmbeddedUpdate() throws Exception {
        AdditionalDataEntity ad = new AdditionalDataEntity();
        ad.setStrValue("test");
        ad.setCounter(12);
        morphium.store(ad);
        Thread.sleep(100);

        morphium.set(getIdQuery(ad), "test", new EmbeddedObject("emb", "key", 123));
        Thread.sleep(100);
        ad = morphium.reread(ad);
        assert (ad.getAdditionals() != null);
        assert (ad.getAdditionals().containsKey("test"));
        assert (ad.getAdditionals().get("test") instanceof EmbeddedObject);
        assert (((EmbeddedObject) ad.getAdditionals().get("test")).getName().equals("emb"));
        checkTypeId(EmbeddedObject.class, ad, "test");
    }


    @Test
    public void testSimple() throws Exception {
        UncachedObject uc = new UncachedObject("str", 123);
        morphium.store(uc);
        assertThat(morphium.createQueryFor(UncachedObject.class).f("_id").eq(uc.getMorphiumId()).asMapList().get(0)
                .get("class_name")).isNull();
    }

    @Test
    public void testList() throws Exception {
        ListContainer lc = new ListContainer();
        lc.addEmbedded(new EmbeddedObject("name", "str", 123));
        lc.setName("embedded object #1");

        UncachedObject uc = new UncachedObject("referenced", 123);
        lc.addRef(uc);

        morphium.store(lc);

        checkTypeId(EmbeddedObject.class, lc, "embedded_object_list.0");
        Map<String, Object> m = getMapForEntity(lc);
        String ref = (String) getValueInPath(m, "ref_list.0.referenced_class_name");
        assertThat(ref).isEqualTo("uc");

    }


    @Test
    public void testListPush() throws Exception {
        ListContainer lc = new ListContainer();
        lc.addEmbedded(new EmbeddedObject("name", "str", 123));
        lc.setName("embedded object #1");

        UncachedObject uc = new UncachedObject("referenced", 123);
        lc.addRef(uc);

        morphium.store(lc);

        morphium.push(lc, ListContainer.Fields.embeddedObjectList, new EmbeddedObject("new one", "value", 1), false);
        Thread.sleep(100);
        checkTypeId(EmbeddedObject.class, lc, "embedded_object_list.0");
        checkTypeId(EmbeddedObject.class, lc, "embedded_object_list.1");

        MorphiumReference r = new MorphiumReference("uc", uc.getMorphiumId());
        morphium.push(lc, ListContainer.Fields.refList, r, false);

        Map<String, Object> m = getMapForEntity(lc);
        String ref = (String) getValueInPath(m, "ref_list.0.referenced_class_name");
        assertThat(ref).isEqualTo("uc");
        ref = (String) getValueInPath(m, "ref_list.1.referenced_class_name");
        assertThat(ref).isEqualTo("uc");

        lc.getEmbeddedObjectList().clear();
        lc.getRefList().clear();
        lc = morphium.reread(lc);

        assertThat(lc.getEmbeddedObjectList().size()).isEqualTo(2);
        assertThat(lc.getRefList().size()).isEqualTo(2);
    }


    @Test
    public void testEmbedded() throws Exception {
        ComplexObject co = new ComplexObject();
        co.setEinText("a text");
        co.setEmbed(new EmbeddedObject("name", "value", 123));
        morphium.store(co);
        //no type id there, as type is determined clearly
        assertThat(getMapForEntity(co).get("embed")).isInstanceOf(Map.class);
        assertThat(((Map) getMapForEntity(co).get("embed")).containsKey("class_name")).isFalse();

        //updating SETs type ID, as during update we do not know if the field has java representation that matches.
        morphium.set(co, ComplexObject.Fields.embed, new EmbeddedObject("new", "Value", 123123));
        assertThat(getMapForEntity(co).get("embed")).isInstanceOf(Map.class);
        checkTypeId(EmbeddedObject.class, co, "embed");

    }


    @Test
    public void testEmbeddedPolymorphSubClass() throws Exception {
        ComplexObject co = new ComplexObject();
        co.setEinText("a text");
        co.setEmbed(new EmbSubClass("name", "value", 123, "mySub"));
        morphium.store(co);

        morphium.reread(co);

        assertThat(co.getEmbed()).isInstanceOf(EmbSubClass.class);
        checkTypeId(EmbSubClass.class, co, "embed");
        assertThat(((Map) getMapForEntity(co).get("embed")).get("class_name")).isEqualTo(EmbSubClass.class.getName());
    }


    private <T> Query<T> getIdQuery(T entity) {
        return (Query<T>) morphium.createQueryFor(entity.getClass()).f("_id").eq(morphium.getId(entity));
    }

    private Map<String, Object> getMapForEntity(Object entity) {
        return morphium.createQueryFor(entity.getClass()).f("_id").eq(morphium.getARHelper().getId(entity)).asMapList().get(0);
    }

    private Object getValueInPath(Map<String, Object> m, String fieldName) {
        String[] path = fieldName.split("\\.");
        Object value = m;
        for (int i = 0; i < path.length; i++) {
            String fld = path[i];
            if (value instanceof Map && ((Map) value).get(fld) instanceof List) {
                int idx = Integer.parseInt(path[i + 1]);
                value = ((List) ((Map) value).get(fld)).get(idx);
                i++;
            } else {
                assertThat(!(m.get(fld) instanceof Map));
                value = ((Map) value).get(fld);
            }
        }
        return value;
    }

    private void checkTypeId(Class cls, Object entity, String fieldName) {
        Map<String, Object> m = getMapForEntity(entity);


        String typeId = cls.getName();
        Entity e = (Entity) cls.getAnnotation(Entity.class);
        if (e != null) {
            typeId = e.typeId();
        } else {
            Embedded emb = (Embedded) cls.getAnnotation(Embedded.class);
            if (emb != null) {
                typeId = emb.typeId();
            }
        }
        if (typeId.equals(".")) {
            typeId = cls.getName();
        }
        if (fieldName.contains(".")) {
            Map value = (Map) getValueInPath(m, fieldName);
            assertThat(value.get("class_name")).isEqualTo(typeId);

        } else {
            assertThat(m.get(fieldName)).isNotNull();
            assertThat(((Map) m.get(fieldName)).get("class_name")).isEqualTo(typeId);
        }
    }

    @Embedded(polymorph = true)
    public static class EmbSubClass extends EmbeddedObject {
        private String subElement;

        public EmbSubClass(String name, String value, long testValueLong, String subElement) {
            super(name, value, testValueLong);
            this.subElement = subElement;
        }

        public String getSubElement() {
            return subElement;
        }

        public void setSubElement(String subElement) {
            this.subElement = subElement;
        }
    }

}
