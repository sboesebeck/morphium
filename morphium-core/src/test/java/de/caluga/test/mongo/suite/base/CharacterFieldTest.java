package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MOR-078: a {@code char} or {@code Character} field is written as a number (as {@code store()} always did) and read back as the same character. Before, the number was
 * refused with "Field has type char got type class java.lang.Integer" when the document was read, so an entity with a {@code char} could be written but not read.
 */
@Tag("core")
public class CharacterFieldTest extends MultiDriverTestBase {

    @Entity
    public static class Letters {
        @Id
        public String id;
        public char letter;
        public Character boxed;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aCharIsReadBackAfterStore(Morphium m) throws Exception {
        try (m) {
            Letters l = new Letters();
            l.id = "l1";
            l.letter = 'x';
            l.boxed = 'Y';
            m.store(l);

            Letters back = m.createQueryFor(Letters.class).f("id").eq("l1").get();

            assertEquals('x', back.letter);
            assertEquals('Y', back.boxed);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aCharIsReadBackAfterAnUpdate(Morphium m) throws Exception {
        try (m) {
            Letters l = new Letters();
            l.id = "l1";
            l.letter = 'a';
            m.store(l);

            m.createQueryFor(Letters.class).f("id").eq("l1").set("letter", 'q');
            m.createQueryFor(Letters.class).f("id").eq("l1").modify().set("boxed", 'Z').updateOne();

            Letters back = m.createQueryFor(Letters.class).f("id").eq("l1").get();
            assertEquals('q', back.letter);
            assertEquals('Z', back.boxed);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aCharCanBeQueried(Morphium m) throws Exception {
        try (m) {
            Letters l = new Letters();
            l.id = "l1";
            l.letter = 'k';
            m.store(l);

            assertEquals(1, m.createQueryFor(Letters.class).f("letter").eq('k').countAll());
            assertEquals(0, m.createQueryFor(Letters.class).f("letter").eq('j').countAll());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aCharIsReadFromAnyNumberTypeAndFromAOneCharacterString(Morphium m) throws Exception {
        try (m) {
            Letters l = new Letters();
            l.id = "l1";
            m.store(l);

            // a document that another writer produced: int64 and double for the numbers, a string for the character
            m.createQueryFor(Letters.class).f("id").eq("l1").set(java.util.Map.of("letter", 100L, "boxed", 90.0), false, false);
            Letters numbers = m.createQueryFor(Letters.class).f("id").eq("l1").get();
            assertEquals('d', numbers.letter);
            assertEquals('Z', numbers.boxed);

            m.createQueryFor(Letters.class).f("id").eq("l1").set(java.util.Map.of("letter", "q", "boxed", "R"), false, false);
            Letters strings = m.createQueryFor(Letters.class).f("id").eq("l1").get();
            assertEquals('q', strings.letter);
            assertEquals('R', strings.boxed);
        }
    }
}
