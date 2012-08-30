package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import junit.framework.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: martinstolz
 * Date: 29.08.12
 */
public class ValidationTest extends MongoTest {

    @Test
    public void testAllValid() {
        ValidationTestObject o = getValidObject();
        MorphiumSingleton.get().store(o);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testNotNull() {
        ValidationTestObject o = getValidObject();
        o.setAnotherInt(null);
        MorphiumSingleton.get().store(o);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testMinMax() {
        ValidationTestObject o = getValidObject();
        o.setTheInt(2);
        MorphiumSingleton.get().store(o);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testMinMaxList() {
        ValidationTestObject o = getValidObject();
        o.getFriends().clear();
        MorphiumSingleton.get().store(o);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEmail() {
        ValidationTestObject o = getValidObject();
        o.setEmail("uh oh this won't validate...");
        MorphiumSingleton.get().store(o);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testDateFuture() {
        ValidationTestObject o = getValidObject();
        o.setWhenever(new Date(System.currentTimeMillis() - 86400000));
        MorphiumSingleton.get().store(o);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testRegex() {
        ValidationTestObject o = getValidObject();
        o.setWhereever("at the beach");
        MorphiumSingleton.get().store(o);
    }

    @Test
    public void testMultipleValidationErrors() {
        ValidationTestObject o = getValidObject();
        o.setWhereever("at the beach");
        o.setWhenever(new Date(System.currentTimeMillis() - 86400000));
        o.setEmail("uh oh this won't validate...");

        try {
            MorphiumSingleton.get().store(o);
        } catch (ConstraintViolationException cfe) {
            Assert.assertTrue("must be three violations", cfe.getConstraintViolations().size() == 3);
        }

    }

    private ValidationTestObject getValidObject() {
        ValidationTestObject o = new ValidationTestObject();
        o.setAnotherInt(123);
        o.setTheInt(4);
        o.setEmail("fish@water.com");

        List friends = new ArrayList();
        friends.add("Angie");
        friends.add("Julian");
        friends.add("Xaver");
        friends.add("Christian");
        o.setFriends(friends);

        o.setWhenever(new Date(System.currentTimeMillis() + 86400000));
        o.setWhereever("münchen");

        return o;
    }

}
