package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.data.MorphiumRepository;
import jakarta.data.repository.Repository;

import java.util.List;

/**
 * Repository extending {@link MorphiumRepository} to test distinct(), morphium() and query() methods.
 */
@Repository
public interface MorphiumItemRepository extends MorphiumRepository<ItemEntity, String> {

    List<ItemEntity> findByTag(String tag);
}
