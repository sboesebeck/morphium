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

    public TransactionalTestService(Morphium morphium) {
        this.morphium = morphium;
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
}
