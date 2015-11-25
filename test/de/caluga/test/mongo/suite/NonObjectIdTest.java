package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.test.mongo.suite.data.Person;
import org.junit.Test;

import java.util.Date;

/**
 * User: Stephan Bösebeck
 * Date: 26.06.13
 * Time: 08:20
 * <p/>
 * TODO: Add documentation here
 */
@SuppressWarnings("AssertWithSideEffects")
public class NonObjectIdTest extends MongoTest {
    @Test
    public void nonObjectIdTest() throws Exception {

        Person p = new Person();
        p.setId("ABC123");
        p.setBirthday(new Date());
        p.setName("1st Test");
        morphium.store(p);

        p = new Person();
        p.setId("BBC123");
        p.setBirthday(new Date());
        p.setName("2nd Test");
        morphium.store(p);
        waitForAsyncOperationToStart(100);
        waitForWrites();

        p.setName("CHANGED");
        morphium.store(p);


        p = new Person();
        p.setBirthday(new Date());
        p.setName("no ID");
        morphium.store(p);
        waitForAsyncOperationToStart(100);
        waitForWrites();

        long cnt = morphium.createQueryFor(Person.class).countAll();
        assert (cnt == 3) : "Count wrong: " + cnt;
        assert (morphium.findById(Person.class, "BBC123").getName().equals("CHANGED"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void nonObjectIdTestFail() throws Exception {
        Tst t = new Tst();
        t.str = "test-a-string";

        morphium.store(t); //will fail
    }

    @Entity
    public static class Tst {
        @Id
        private Long myId;
        private String str;
    }
}
