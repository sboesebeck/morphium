package de.caluga.morphium.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MOR-089: a string literal of one character is compared with a {@code char} or {@code Character} field as the character (a character is stored as its number): {@code letter = 'b'},
 * {@code letter IN ('a', 'c')}, {@code letter BETWEEN 'a' AND 'b'}.
 */
class JdqlCharLiteralTest {

    @Entity
    static class Glyph {
        @Id
        private long id;
        private char letter;
        private Character boxed;
        private String text;

        Glyph() {}

        Glyph(long id, char letter, String text) {
            this.id = id;
            this.letter = letter;
            this.boxed = letter;
            this.text = text;
        }
    }

    static class GlyphRepo extends AbstractMorphiumRepository<Glyph, Long> {
        GlyphRepo(Morphium morphium) {
            super(new RepositoryMetadata(Glyph.class, Long.class, "id"));
            setMorphium(morphium);
        }
    }

    private Morphium morphium;
    private GlyphRepo repo;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor089_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new GlyphRepo(morphium);
        morphium.store(new Glyph(1, 'a', "a"));
        morphium.store(new Glyph(2, 'b', "b"));
        morphium.store(new Glyph(3, 'c', "c"));
        morphium.store(new Glyph(4, 'd', "d"));
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @SuppressWarnings("unchecked")
    private List<Long> ids(String jdql) {
        Object result = JdqlMethodBridge.executeJdql(repo, jdql, "", -1, -1, -1, -1, new Object[0], false, false, false, false, false, "", false, null);
        return ((List<Glyph>) result).stream().map(g -> g.id).sorted().toList();
    }

    @Test
    @DisplayName("equality, IN, NOT IN and BETWEEN with one-character literals on a char and a Character field")
    void literals() {
        assertThat(ids("WHERE letter = 'b'")).containsExactly(2L);
        assertThat(ids("WHERE boxed = 'c'")).containsExactly(3L);
        assertThat(ids("WHERE letter IN ('a', 'c')")).containsExactly(1L, 3L);
        assertThat(ids("WHERE letter NOT IN ('a', 'c')")).containsExactly(2L, 4L);
        assertThat(ids("WHERE letter BETWEEN 'b' AND 'c'")).containsExactly(2L, 3L);
        assertThat(ids("WHERE letter <> 'a' AND boxed > 'b'")).containsExactly(3L, 4L);
    }

    @Test
    @DisplayName("a string field keeps comparing strings")
    void stringFieldIsUnchanged() {
        assertThat(ids("WHERE text = 'b'")).containsExactly(2L);
        assertThat(ids("WHERE text IN ('a', 'd')")).containsExactly(1L, 4L);
    }
}
