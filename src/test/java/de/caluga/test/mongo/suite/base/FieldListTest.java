package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.ReadOnly;
import de.caluga.morphium.driver.commands.StoreMongoCommand;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 25.04.14
 * Time: 11:43
 * To change this template use File | Settings | File Templates.
 */
@Tag("core")
public class FieldListTest extends MorphiumTestBase {

    @Test
    public void testFieldList() {
        createUncachedObjects(100);
        TestUtils.waitForWrites(morphium, log);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.addProjection(UncachedObject.Fields.counter);

        q = q.f(UncachedObject.Fields.counter).eq(30);
        UncachedObject uc = q.get();
        assert (uc.getStrValue() == null) : "Value is " + uc.getStrValue();
    }

    @Test
    public void testReadOnly() throws Exception {
        morphium.dropCollection(ReadOnlyObject.class);
        ReadOnlyObject ro = new ReadOnlyObject();
        ro.setStrValue("ReadOnlyTest");
        ro.setCounter(100);
        ro.readOnlyValue = "Must not be stored!";

        morphium.store(ro);
        Thread.sleep(250);
        ro = morphium.reread(ro);

        assertNull(ro.readOnlyValue, "Value wrong: " + ro.readOnlyValue);

        ro.setStrValue("OtherValue");
        ro.readOnlyValue = "must still not be stored, even after update!";
        morphium.store(ro);
        morphium.reread(ro);
        assert (ro.readOnlyValue == null);

        //forcing store of a value
        Map<String, Object> marshall = morphium.getMapper().serialize(ro);
        marshall.put("read_only_value", "stored in db");
        List<Map<String, Object>> lst = new ArrayList<>();
        lst.add(marshall);
        StoreMongoCommand cmd = new StoreMongoCommand(morphium.getDriver().getPrimaryConnection(null));
        cmd.setDb(morphium.getDatabase()).setColl("read_only_object").setDocuments(lst);
        cmd.execute();
        cmd.releaseConnection();
        Thread.sleep(100);
        morphium.reread(ro);
        assert (ro.readOnlyValue.equals("stored in db"));

        ro.readOnlyValue = "different";
        morphium.reread(ro);
        assert (ro.readOnlyValue.equals("stored in db"));

    }


    public static class ReadOnlyObject extends UncachedObject {
        @ReadOnly
        private String readOnlyValue;
    }

}
