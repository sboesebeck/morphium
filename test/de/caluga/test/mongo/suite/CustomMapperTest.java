package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CustomMappedObject;
import de.caluga.test.mongo.suite.data.CustomMappedObjectMapper;
import de.caluga.test.mongo.suite.data.ObjectWithCustomMappedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 17:17
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
public class CustomMapperTest extends MongoTest {

    private final int count = 2;

    @Test
    public void customMappedObjectTest() throws Exception {
        morphium.registerTypeMapper(CustomMappedObject.class, new CustomMappedObjectMapper());
        morphium.dropCollection(ObjectWithCustomMappedObject.class);

        ObjectWithCustomMappedObject containingObject = new ObjectWithCustomMappedObject();

        List<CustomMappedObject> list = new ArrayList<>();
        Map<String, CustomMappedObject> map = new HashMap<>();

        mockupContainerObject(containingObject, list, map);

        morphium.store(containingObject);

        Query<ObjectWithCustomMappedObject> q = morphium.createQueryFor(ObjectWithCustomMappedObject.class).f("id").eq(containingObject.getId());
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        ObjectWithCustomMappedObject readContainingObject = q.get();

        assert (readContainingObject != null) : "Error - not found?";

        assert (readContainingObject.getCustomMappedObject() != null) : "Custom mapped object null?";
        assert (readContainingObject.getCustomMappedObjectList() != null) : "List of custom mapped object null?";
        assert (readContainingObject.getCustomMappedObjectMap() != null) : "Map with custom mapped object null?";

        assert (readContainingObject.getCustomMappedObjectList().size() == count) : "List of custom mapped objects has wrong size?";
        assert (readContainingObject.getCustomMappedObjectMap().size() == count) : "Map with custom mapped objects as value has wrong size?";

        assert (readContainingObject.getCustomMappedObject().equals(containingObject.getCustomMappedObject())) : "Single custom mapped objects differ?";

        for (int i = 0; i < count; i++) {
            CustomMappedObject referenceObject = containingObject.getCustomMappedObjectList().get(i);

            assert (readContainingObject.getCustomMappedObjectList().get(i) != null) : "Custom mapped object in list missing? - " + i;
            assert (readContainingObject.getCustomMappedObjectList().get(i).equals(referenceObject)) : "Custom mapped objects in list differ? - " + i;
            assert (readContainingObject.getCustomMappedObjectMap().get(referenceObject.getName()).equals(map.get(referenceObject.getName()))) : "Custom mapped objects in map differ? - " + i;
        }

        morphium.deregisterTypeMapper(CustomMappedObject.class);
    }

    private void mockupContainerObject(
            ObjectWithCustomMappedObject containingObject,
            List<CustomMappedObject> list,
            Map<String, CustomMappedObject> map) {
        CustomMappedObject singleCustomMappedObject = new CustomMappedObject();
        singleCustomMappedObject.setName("customMapped");
        singleCustomMappedObject.setValue("single");
        singleCustomMappedObject.setIntValue(-1);

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
    public void complexCustomMappingTest() throws Exception {
        morphium.registerTypeMapper(CustomMappedObject.class, new CustomMappedObjectMapper());
        morphium.dropCollection(ObjectWithCustomMappedObject.class);

        ComplexCustomMapperObject containingObject = new ComplexCustomMapperObject();

        List<Map<String, List<CustomMappedObject>>> complexestList = new ArrayList<>();
        Map<String, List<Map<String, CustomMappedObject>>> complexestMap = new HashMap<>();

        List<Map<String, CustomMappedObject>> complexList = new ArrayList<>();
        Map<String, List<CustomMappedObject>> complexMap = new HashMap<>();

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

        assert (readContainingObject != null) : "Error - not found?";

        assert (readContainingObject.getComplexList() != null) : "Complex list object null?";
        assert (readContainingObject.getComplexMap() != null) : "Complex map object null?";
        assert (readContainingObject.getComplexestList() != null) : "Complexest list object null?";
        assert (readContainingObject.getComplexestMap() != null) : "Complexest map object null?";

        assert (readContainingObject.getComplexList().size() == 1) : "Complex list has wrong size?";
        assert (readContainingObject.getComplexMap().size() == 1) : "Complex map has wrong size?";
        assert (readContainingObject.getComplexestList().size() == 1) : "Complexest list has wrong size?";
        assert (readContainingObject.getComplexestMap().size() == 1) : "Complexest map has wrong size?";

        assert (readContainingObject.getComplexList().equals(complexList)) : "Complex lists differ?";
        assert (readContainingObject.getComplexMap().equals(complexMap)) : "Complex maps differ?";
        assert (readContainingObject.getComplexestList().equals(complexestList)) : "Complexest lists differ?";
        assert (readContainingObject.getComplexestMap().equals(complexestMap)) : "Complexest maps differ?";

        morphium.deregisterTypeMapper(CustomMappedObject.class);

    }

    private static class ComplexCustomMapperObject extends ObjectWithCustomMappedObject {
        private List<Map<String, CustomMappedObject>> complexList;
        private Map<String, List<CustomMappedObject>> complexMap;

        private List<Map<String, List<CustomMappedObject>>> complexestList;
        private Map<String, List<Map<String, CustomMappedObject>>> complexestMap;

        public List<Map<String, CustomMappedObject>> getComplexList() {
            return complexList;
        }

        public void setComplexList(
                List<Map<String, CustomMappedObject>> complexList) {
            this.complexList = complexList;
        }

        public Map<String, List<CustomMappedObject>> getComplexMap() {
            return complexMap;
        }

        public void setComplexMap(
                Map<String, List<CustomMappedObject>> complexMap) {
            this.complexMap = complexMap;
        }

        public List<Map<String, List<CustomMappedObject>>> getComplexestList() {
            return complexestList;
        }

        public void setComplexestList(
                List<Map<String, List<CustomMappedObject>>> complexestList) {
            this.complexestList = complexestList;
        }

        public Map<String, List<Map<String, CustomMappedObject>>> getComplexestMap() {
            return complexestMap;
        }

        public void setComplexestMap(
                Map<String, List<Map<String, CustomMappedObject>>> complexestMap) {
            this.complexestMap = complexestMap;
        }
    }
}
