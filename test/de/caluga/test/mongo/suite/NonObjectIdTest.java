package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import org.junit.Test;

import java.util.Date;

/**
 * User: Stephan Bösebeck
 * Date: 26.06.13
 * Time: 08:20
 * <p/>
 * TODO: Add documentation here
 */
public class NonObjectIdTest extends MongoTest {
    @Test
    public void nonObjectIdTest() throws Exception {

        Person p = new Person();
        p.setId("ABC123");
        p.setBirthday(new Date());
        p.setName("1st Test");
        MorphiumSingleton.get().store(p);

        p = new Person();
        p.setId("BBC123");
        p.setBirthday(new Date());
        p.setName("2nd Test");
        MorphiumSingleton.get().store(p);
        waitForAsyncOperationToStart(100);
        waitForWrites();

        p.setName("CHANGED");
        MorphiumSingleton.get().store(p);


        p = new Person();
        p.setBirthday(new Date());
        p.setName("no ID");
        MorphiumSingleton.get().store(p);
        waitForAsyncOperationToStart(100);
        waitForWrites();

        long cnt = MorphiumSingleton.get().createQueryFor(Person.class).countAll();
        assert (cnt == 3) : "Count wrong: " + cnt;
        assert (MorphiumSingleton.get().findById(Person.class, "BBC123").getName().equals("CHANGED"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void nonObjectIdTestFail() throws Exception {
        Tst t = new Tst();
        t.str = "test-a-string";

        MorphiumSingleton.get().store(t); //will fail
    }

    @Entity
    public static class Tst {
        @Id
        private Long myId;
        private String str;
    }
}
