package de.caluga.test.morphium;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that non-transient driver errors (e.g. duplicate key violations)
 * are propagated immediately as {@link MorphiumDriverException} rather than
 * being retried as network errors by {@code MorphiumWriterImpl.submitAndBlockIfNecessary()}.
 * <p>
 * All tests run with InMemoryDriver — no MongoDB required.
 */
@Tag("inmemory")
public class MorphiumDriverExceptionPropagationTest {

    @Entity(collectionName = "unique_entity")
    public static class UniqueEntity {
        @Id
        public String id;

        @Index(options = {"unique:true"})
        public String uniqueKey;

        public String value;

        public UniqueEntity() {}

        public UniqueEntity(String id, String uniqueKey, String value) {
            this.id = id;
            this.uniqueKey = uniqueKey;
            this.value = value;
        }
    }

    private Morphium morphium;

    @BeforeEach
    public void setup() {
        MorphiumConfig cfg = new MorphiumConfig("driver_exception_test_db", 10, 10_000, 1_000);
        cfg.setDriverName(InMemoryDriver.driverName);
        // Configure retries > 0 so we can verify the exception is NOT retried
        cfg.setRetriesOnNetworkError(3);
        morphium = new Morphium(cfg);
        morphium.ensureIndicesFor(UniqueEntity.class);
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Test
    public void duplicateKey_throwsMorphiumDriverException_notRetried() {
        UniqueEntity first = new UniqueEntity("id-1", "same-key", "first");
        morphium.store(first);

        // Second entity with same uniqueKey should fail immediately
        UniqueEntity duplicate = new UniqueEntity("id-2", "same-key", "duplicate");

        long startMs = System.currentTimeMillis();
        assertThatThrownBy(() -> morphium.store(duplicate))
            .isInstanceOf(MorphiumDriverException.class);
        long elapsedMs = System.currentTimeMillis() - startMs;

        // If the error were retried (with sleep between retries), it would take
        // significantly longer. The default sleep is 1000ms * 3 retries = 3s.
        // A direct throw should complete in well under 1 second.
        assertThat(elapsedMs)
            .as("duplicate key error should be thrown immediately, not retried")
            .isLessThan(2000);
    }

    @Test
    public void duplicateKey_existingEntity_notCorrupted() {
        UniqueEntity first = new UniqueEntity("id-1", "same-key", "first");
        morphium.store(first);

        UniqueEntity duplicate = new UniqueEntity("id-2", "same-key", "duplicate");
        try {
            morphium.store(duplicate);
        } catch (MorphiumDriverException expected) {
            // expected
        }

        // Verify the original entity is still intact
        UniqueEntity loaded = morphium.createQueryFor(UniqueEntity.class)
            .f("id").eq("id-1")
            .get();
        assertThat(loaded).isNotNull();
        assertThat(loaded.uniqueKey).isEqualTo("same-key");
        assertThat(loaded.value).isEqualTo("first");

        // Verify only one entity exists
        long count = morphium.createQueryFor(UniqueEntity.class).countAll();
        assertThat(count).isEqualTo(1);
    }
}
