package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import org.springframework.stereotype.Service;

/**
 * Second transactional bean, so {@link TransactionalTestService} can nest a
 * {@code @MorphiumTransactional} call through a real Spring proxy rather than a
 * self-invocation (which would bypass the aspect entirely and prove nothing).
 */
@Service
public class NestedTransactionalTestService {

    private final Morphium morphium;

    public NestedTransactionalTestService(Morphium morphium) {
        this.morphium = morphium;
    }

    @MorphiumTransactional
    public void saveInner(TestEntity entity) {
        morphium.store(entity);
    }

    @MorphiumTransactional
    public void saveInnerThenThrow(TestEntity entity) {
        morphium.store(entity);
        throw new IllegalStateException("forced failure inside the nested transaction");
    }
}
