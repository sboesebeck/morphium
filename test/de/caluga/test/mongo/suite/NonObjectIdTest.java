package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
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
    public void updateTest() throws Exception {
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
        waitForAsyncOperationToStart(100);
        waitForWrites();

        assert (MorphiumSingleton.get().createQueryFor(Person.class).countAll() == 2);
        assert (MorphiumSingleton.get().findById(Person.class, "BBC123").getName().equals("CHANGED"));

    }
}
