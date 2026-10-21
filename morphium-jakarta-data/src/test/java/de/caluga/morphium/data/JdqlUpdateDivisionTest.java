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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * MOR-077: {@code UPDATE Coordinate SET y = y / :d} is a multiplication by the inverse. A divisor of 0 in a parameter is refused before anything is written.
 */
class JdqlUpdateDivisionTest {

    @Entity
    static class Coordinate {
        @Id
        private String id;
        private double x;
        private float y;

        Coordinate() {}

        Coordinate(String id, double x, float y) {
            this.id = id;
            this.x = x;
            this.y = y;
        }
    }

    static class CoordinateRepo extends AbstractMorphiumRepository<Coordinate, String> {
        CoordinateRepo(Morphium morphium) {
            super(new RepositoryMetadata(Coordinate.class, String.class, "id"));
            setMorphium(morphium);
        }
    }

    private Morphium morphium;
    private CoordinateRepo repo;

    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("mor077div_" + System.nanoTime());
        cfg.addHostToSeed("localhost");
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        repo = new CoordinateRepo(morphium);
        morphium.store(new Coordinate("a", 2.2, 2.34f));
        morphium.store(new Coordinate("b", 1.41, 5.25f));
    }

    @AfterEach
    void tearDown() {
        morphium.close();
    }

    @Test
    @DisplayName("a division by a parameter and by a literal, together with an assignment")
    void divides() {
        long matched = JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Coordinate SET x = :newX, y = y / :d WHERE id = :id", "newX:0,d:1,id:2", new Object[] {1.23, 1.5f, "a"});

        assertThat(matched).isEqualTo(1);
        Coordinate a = morphium.findById(Coordinate.class, "a", null);
        assertThat(a.x).isCloseTo(1.23, within(0.001));
        assertThat(a.y).isCloseTo(1.56f, within(0.001f));
        assertThat(morphium.findById(Coordinate.class, "b", null).y).isEqualTo(5.25f);

        JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Coordinate SET x = x / 4 WHERE id = 'b'", "", new Object[0]);
        assertThat(morphium.findById(Coordinate.class, "b", null).x).isCloseTo(0.3525, within(0.0001));
    }

    @Test
    @DisplayName("a divisor of 0 in a parameter is refused and nothing is written")
    void dividesByZero() {
        assertThatThrownBy(() -> JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Coordinate SET x = 9, y = y / :d", "d:0", new Object[] {0})).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("divides by zero");
        assertThat(morphium.findById(Coordinate.class, "a", null).x).isEqualTo(2.2);
    }

    @Test
    @DisplayName("literals with a suffix in a SET item and in the WHERE clause, a long literal with a fraction is refused")
    void suffixes() {
        assertThat(JdqlMethodBridge.executeJdqlUpdate(repo, "UPDATE Coordinate SET x = 7.5d, y = y * 2f WHERE x > 0.0d AND y > 0.0f", "", new Object[0])).isEqualTo(2);
        assertThat(morphium.findById(Coordinate.class, "a", null).x).isEqualTo(7.5);
        assertThat(morphium.findById(Coordinate.class, "b", null).y).isCloseTo(10.5f, within(0.001f));
        assertThatThrownBy(() -> JdqlUpdate.parse("UPDATE Coordinate SET x = 1.5L")).isInstanceOf(IllegalArgumentException.class);
    }
}
