package de.caluga.test.mongo.suite.unit;

import de.caluga.morphium.bulk.MorphiumBulkContext;
import de.caluga.test.mongo.suite.MongoTest;
import de.caluga.test.mongo.suite.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Created by stephan on 18.11.15.
 */
public class MorphiumBulkContextTest extends MongoTest {

    @Test
    public void testRunBulk() throws Exception {


    }

    @Test
    public void testRunBulk1() throws Exception {

    }

    @Test
    public void testAddInsertRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addInsertRequest(new ArrayList<>());
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddDeleteRequest() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addDeleteRequest(new ArrayList<>());
        assert (c.getNumberOfRequests() == 0);
        ArrayList<UncachedObject> lst = new ArrayList<>();
        lst.add(new UncachedObject());
        c.addDeleteRequest(lst);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddDeleteRequest1() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addDeleteRequest(new UncachedObject());
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddDeleteRequest2() throws Exception {
        MorphiumBulkContext<UncachedObject> c = morphium.createBulkRequestContext(UncachedObject.class, false);
        c.addDeleteRequest(morphium.createQueryFor(UncachedObject.class), false);
        assert (c.getNumberOfRequests() == 1);
    }

    @Test
    public void testAddCustomUpdateRequest() throws Exception {

    }

    @Test
    public void testAddSetRequest() throws Exception {

    }

    @Test
    public void testAddUnSetRequest() throws Exception {

    }

    @Test
    public void testAddSetRequest1() throws Exception {

    }

    @Test
    public void testAddUnsetRequest() throws Exception {

    }

    @Test
    public void testAddIncRequest() throws Exception {

    }

    @Test
    public void testAddIncRequest1() throws Exception {

    }

    @Test
    public void testAddCurrentDateRequest() throws Exception {

    }

    @Test
    public void testAddCurrentDateRequest1() throws Exception {

    }

    @Test
    public void testAddMinRequest() throws Exception {

    }

    @Test
    public void testAddMinRequest1() throws Exception {

    }

    @Test
    public void testAddMinRequest2() throws Exception {

    }

    @Test
    public void testAddMaxRequest() throws Exception {

    }

    @Test
    public void testAddMaxRequest1() throws Exception {

    }

    @Test
    public void testAddMaxRequest2() throws Exception {

    }

    @Test
    public void testAddRenameRequest() throws Exception {

    }

    @Test
    public void testAddRenameRequest1() throws Exception {

    }

    @Test
    public void testAddMulRequest() throws Exception {

    }

    @Test
    public void testAddMulRequest1() throws Exception {

    }

    @Test
    public void testAddPopRequest() throws Exception {

    }

    @Test
    public void testAddPopRequest1() throws Exception {

    }

    @Test
    public void testAddPushRequest() throws Exception {

    }

    @Test
    public void testAddPushRequest1() throws Exception {

    }

    @Test
    public void testAddSetRequest2() throws Exception {

    }

    @Test
    public void testAddUnsetRequest1() throws Exception {

    }

    @Test
    public void testAddIncRequest2() throws Exception {

    }

    @Test
    public void testAddPushRequest2() throws Exception {

    }
}