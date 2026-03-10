package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.FilterExpression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: Hans Karlsson
 * Date: 21.11.12
 * Time: 20:17
 * <p>
 */
@SuppressWarnings("unchecked")
@Tag("core")
public class FilterExpressionTest {

    private FilterExpression fe;

    @BeforeEach
    public void setup() {
        fe = new FilterExpression();
        fe.setField("field");
        fe.setValue("value");
    }

    @Test
    public void testNullValue() {
        fe.setValue(null);
        Map<String, Object> dbObject = fe.dbObject();
        assert (dbObject.containsKey("field"));
        assert (dbObject.get("field") == null);
    }

    @Test
    public void testAddTwoChildren() {
        fe.addChild(createChild1());
        fe.addChild(createChild2());
        assert (fe.getChildren().size() == 2);
    }

    @Test
    public void testAddListWithTwoChildren() {
        fe.setChildren(createChildrenList());
        assert (fe.getChildren().size() == 2);
    }

    @Test
    public void testDBObjectWithSingleValue() {
        Map map = fe.dbObject();

        String key = (String) map.keySet().iterator().next();
        String value = (String) map.values().iterator().next();
        assert (map.keySet().size() == 1);
        assert ("field".equals(key));
        assert ("value".equals(value));
    }

    private enum TestEnum {
        FIRST_ENUM, SECOND_ENUM
    }

    @Test
    public void testDBObjectWithSingleEnumAsValue() {
        TestEnum testEnum = TestEnum.FIRST_ENUM;
        FilterExpression enumFilter = new FilterExpression();
        enumFilter.setField("field");
        enumFilter.setValue(testEnum);

        Map map = enumFilter.dbObject();

        String key = (String) map.keySet().iterator().next();
        String value = (String) map.values().iterator().next();
        assert (map.keySet().size() == 1);
        assert ("field".equals(key));
        assert (testEnum.name().equals(value));
    }

    @Test
    public void testDBObjectWithTwoChildren() {
        fe.addChild(createChild1());
        fe.addChild(createChild2());

        assert ("field".equals(fe.getField()));

        Map map = fe.dbObject();
        assert (map.keySet().size() == 1);
        assert (map.keySet().iterator().next().equals("field"));
        assert (map.values().size() == 1);

        Set fetchedKeys = ((Map<String, Object>) map.values().iterator().next()).keySet();

        assert (fetchedKeys.contains("child1Field") && fetchedKeys.contains("child2Field"));
        assert (((Map<String, Object>) map.values().iterator().next()).get("child1Field").equals("child1Value"));
        assert (((Map<String, Object>) map.values().iterator().next()).get("child2Field").equals("child2Value"));
    }

    @Test
    public void testAddChildTwoTimesShouldBeEquivalentWithAddChildren() {
        FilterExpression fe1 = new FilterExpression();
        fe1.setField("field");
        fe1.addChild(createChild1());
        fe1.addChild(createChild2());

        FilterExpression fe2 = new FilterExpression();
        fe2.setField("field");
        fe2.setChildren(createChildrenList());

        assert (fe1.dbObject().equals(fe2.dbObject()));
    }

    private List<FilterExpression> createChildrenList() {
        List<FilterExpression> filterList = new ArrayList<>();
        filterList.add(createChild1());
        filterList.add(createChild2());
        return filterList;
    }

    private FilterExpression createChild2() {
        FilterExpression child2 = new FilterExpression();
        child2.setField("child2Field");
        child2.setValue("child2Value");
        return child2;
    }

    private FilterExpression createChild1() {
        FilterExpression child = new FilterExpression();
        child.setField("child1Field");
        child.setValue("child1Value");
        return child;
    }

}
