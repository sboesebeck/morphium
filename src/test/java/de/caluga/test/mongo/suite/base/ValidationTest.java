package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.ValidationTestObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * User: martinstolz
 * Date: 29.08.12
 */
@Tag("core")
public class ValidationTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testAllValid(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        ValidationTestObject o = getValidObject();
        morphium.store(o);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testNotNull(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        assertThrows(ConstraintViolationException.class, ()-> {
            ValidationTestObject o = getValidObject();
            o.setAnotherInt(null);
            morphium.store(o);
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testMinMax(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        assertThrows(ConstraintViolationException.class, ()-> {
            ValidationTestObject o = getValidObject();
            o.setTheInt(2);
            morphium.store(o);
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testMinMaxList(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        assertThrows(ConstraintViolationException.class, ()-> {
            ValidationTestObject o = getValidObject();
            o.getFriends().clear();
            morphium.store(o);
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testEmail(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        assertThrows(ConstraintViolationException.class, ()-> {
            ValidationTestObject o = getValidObject();
            o.setEmail("uh oh this won't validate...");
            morphium.store(o);
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testDateFuture(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        assertThrows(ConstraintViolationException.class, ()-> {
            ValidationTestObject o = getValidObject();
            o.setWhenever(new Date(System.currentTimeMillis() - 86400000));
            morphium.store(o);
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testRegex(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        assertThrows(ConstraintViolationException.class, ()-> {
            ValidationTestObject o = getValidObject();
            o.setWhereever("at the beach");
            morphium.store(o);
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testMultipleValidationErrors(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        assertThrows(ConstraintViolationException.class, ()-> {
            ValidationTestObject o = getValidObject();
            o.setWhereever("at the beach");
            o.setWhenever(new Date(System.currentTimeMillis() - 86400000));
            o.setEmail("uh oh this won't validate...");


            morphium.store(o);
        });
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void testEmbeddedObjectsValidationErrors(Morphium morphium) {
        if (!morphium.isValidationEnabled()) return;
        assertThrows(ConstraintViolationException.class, ()-> {
            ValidationTestObject o = getValidObject();
            o.setWhereever("nix");
            ListValidationTestObject lst = new ListValidationTestObject();
            List<ValidationTestObject> obj = new ArrayList<>();
            obj.add(o);
            obj.add(getValidObject());
            obj.add(getValidObject());

            lst.setLst(obj);

            morphium.store(lst);
        });
    }

    private ValidationTestObject getValidObject() {
        ValidationTestObject o = new ValidationTestObject();
        o.setAnotherInt(123);
        o.setTheInt(4);
        o.setEmail("fish@water.com");
        List<String> friends = new ArrayList<>();
        friends.add("Angie");
        friends.add("Julian");
        friends.add("Xaver");
        friends.add("Christian");
        o.setFriends(friends);
        o.setWhenever(new Date(System.currentTimeMillis() + 86400000));
        o.setWhereever("münchen");
        return o;
    }


    @Entity
    public class ListValidationTestObject {
        private List<ValidationTestObject> lst;
        @Id
        private MorphiumId id;

        public List<ValidationTestObject> getLst() {
            return lst;
        }

        public void setLst(List<ValidationTestObject> lst) {
            this.lst = lst;
        }
    }

}
