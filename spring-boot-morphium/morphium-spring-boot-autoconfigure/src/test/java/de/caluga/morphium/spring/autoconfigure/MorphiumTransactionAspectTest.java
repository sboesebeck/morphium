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
}
