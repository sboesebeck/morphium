package de.caluga.morphium.quarkus.data;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.data.AbstractMorphiumRepository;
import de.caluga.morphium.data.RepositoryMetadata;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

/**
 * Quarkus-specific subclass of {@link AbstractMorphiumRepository} that injects
 * the {@link Morphium} instance via CDI {@code @Inject}.
 * <p>
 * Gizmo-generated repository implementations extend this class instead of
 * {@link AbstractMorphiumRepository} directly, so that the Morphium instance
 * is automatically injected by the Quarkus CDI container.
 *
 * @param <T> the entity type
 * @param <K> the primary-key type
 */
public abstract class QuarkusMorphiumRepository<T, K> extends AbstractMorphiumRepository<T, K> {

    @Inject
    Morphium morphium;

    protected QuarkusMorphiumRepository(RepositoryMetadata metadata) {
        super(metadata);
    }

    @PostConstruct
    void init() {
        setMorphium(morphium);
    }

    @Override
    public Morphium getMorphium() {
        return morphium;
    }
}
