package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.test.mongo.suite.data.Person;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * User: Stephan Bösebeck
 * Date: 26.06.13
 * Time: 08:20
 * <p/>
 * TODO: Add documentation here
 */
@SuppressWarnings("AssertWithSideEffects")
public class NonObjectIdTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void nonObjectIdTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("----> Running test with: " + morphium.getDriver().getName());;
            morphium.dropCollection(Person.class);
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
            waitForAsyncOperationsToStart(morphium, 1000);
            TestUtils.waitForWrites(morphium, log);
            p.setName("CHANGED");
            morphium.store(p);
            p = new Person();
            p.setBirthday(new Date());
            p.setName("no ID");
            morphium.store(p);
            waitForAsyncOperationsToStart(morphium, 1000);
            TestUtils.waitForWrites(morphium, log);
            Thread.sleep(1500);
            long cnt = morphium.createQueryFor(Person.class).countAll();
            assert(cnt == 3) : "Count wrong: " + cnt;
            assert(morphium.findById(Person.class, "BBC123").getName().equals("CHANGED"));
        }
    }

    @Entity
    public static class Tst {
        @Id
        private Long myId;
        private String str;
    }
}
