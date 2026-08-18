package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import org.springframework.stereotype.Service;

/**
 * Test-only service exercising {@link MorphiumTransactional} through the
 * {@link MorphiumTransactionAspect}, to verify the aspect is actually woven in
 * when this module is used as an external starter dependency (see
 * {@link MorphiumTransactionAspect}'s class-level documentation for why a plain
 * {@code @Component} would not have been picked up in that scenario).
 */
@Service
public class TransactionalTestService {

    private final Morphium morphium;
    private final NestedTransactionalTestService nested;

    public TransactionalTestService(Morphium morphium, NestedTransactionalTestService nested) {
        this.morphium = morphium;
        this.nested = nested;
    }

    @MorphiumTransactional
    public void saveWithinTransaction(TestEntity entity) {
        morphium.store(entity);
    }

    @MorphiumTransactional
    public void saveThenThrow(TestEntity entity) {
        morphium.store(entity);
        throw new IllegalStateException("forced failure to exercise abortTransaction()");
    }

    /**
     * Outer transactional method calling a second {@code @MorphiumTransactional} bean.
     * The inner call goes through the injected proxy (not {@code this}), so the aspect
     * really does run twice - which is exactly the nesting case REQUIRED propagation has
     * to survive. Without it, the inner {@code startTransaction()} throws and the outer
     * transaction is aborted, losing {@code outer} as well.
     */
    @MorphiumTransactional
    public void saveOuterThenNestedInner(TestEntity outer, TestEntity inner) {
        morphium.store(outer);
        nested.saveInner(inner);
    }

    /**
     * Same nesting, but the inner method throws. The exception must propagate out of the
     * outer method and the outer work must be rolled back - the inner call must not have
     * committed anything on its own.
     */
    @MorphiumTransactional
    public void saveOuterThenNestedInnerThrows(TestEntity outer, TestEntity inner) {
        morphium.store(outer);
        nested.saveInnerThenThrow(inner);
    }
}
