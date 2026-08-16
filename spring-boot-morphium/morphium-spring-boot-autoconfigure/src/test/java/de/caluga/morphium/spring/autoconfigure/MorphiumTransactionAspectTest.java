package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Regression tests for {@link MorphiumTransactionAspect}. Verifies that the aspect is
 * actually registered as a Spring bean (it previously relied on component scan,
 * which never finds it when this module is used as an external starter dependency
 * outside its own package — see the class-level documentation on
 * {@link MorphiumTransactionAspect} for the fix), and that
 * {@code @MorphiumTransactional} methods actually run inside a transaction.
 */
@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("test")
class MorphiumTransactionAspectTest {

    @Autowired(required = false)
    MorphiumTransactionAspect aspect;

    @Autowired
    TransactionalTestService service;

    @Autowired
    Morphium morphium;

    @BeforeEach
    void cleanUp() {
        morphium.clearCollection(TestEntity.class);
    }

    @Test
    void aspectBeanIsRegistered() {
        // Regression: previously a plain @Component, never picked up by component
        // scan for a real application depending on this module as an external jar.
        assertNotNull(aspect, "MorphiumTransactionAspect must be registered as an "
                + "auto-configuration bean, not rely on component scan");
    }

    @Test
    void transactionalMethodCommitsOnNormalReturn() {
        service.saveWithinTransaction(new TestEntity("a", "active", 1));

        assertEquals(1, morphium.createQueryFor(TestEntity.class).countAll());
    }

    @Test
    void transactionalMethodAbortsOnException() {
        assertThrows(IllegalStateException.class,
                () -> service.saveThenThrow(new TestEntity("a", "active", 1)));

        // InMemDriver's abortTransaction() rolls back writes made within the
        // transaction -- if the aspect were never woven in (the original bug), the
        // store() call would have committed outside any transaction and this
        // document would still be present.
        assertEquals(0, morphium.createQueryFor(TestEntity.class).countAll());
    }

    // ---- Review finding 6: nested @MorphiumTransactional lost the outer transaction ----

    @Test
    void nestedTransactionalCallJoinsTheOuterTransaction() {
        // The inner call goes through a second proxied bean, so the aspect really runs twice.
        // Without REQUIRED propagation the inner startTransaction() threw
        // IllegalArgumentException ("transaction in progress"), that exception propagated into
        // the outer advice's catch, and the outer transaction was aborted -- so NEITHER
        // document survived. Both must be present now.
        service.saveOuterThenNestedInner(
                new TestEntity("outer", "active", 1),
                new TestEntity("inner", "active", 2));

        assertEquals(2, morphium.createQueryFor(TestEntity.class).countAll());
    }

    @Test
    void nestedTransactionalRollsBackBothOnInnerFailure() {
        // The inner method throws while joined to the outer transaction. The exception must
        // reach the caller, and because the inner call neither committed nor aborted on its
        // own, the outer advice's abort has to roll back the outer AND the inner write.
        assertThrows(IllegalStateException.class,
                () -> service.saveOuterThenNestedInnerThrows(
                        new TestEntity("outer", "active", 1),
                        new TestEntity("inner", "active", 2)));

        assertEquals(0, morphium.createQueryFor(TestEntity.class).countAll());
    }
}
