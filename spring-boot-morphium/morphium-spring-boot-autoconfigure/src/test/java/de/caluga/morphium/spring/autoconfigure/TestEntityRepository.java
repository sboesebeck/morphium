package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.data.MorphiumRepository;
import de.caluga.morphium.driver.MorphiumId;
import jakarta.data.repository.Repository;

import java.util.List;

@Repository
public interface TestEntityRepository extends MorphiumRepository<TestEntity, MorphiumId> {

    List<TestEntity> findByStatus(String status);

    List<TestEntity> findByStatusAndPriority(String status, int priority);

    long countByStatus(String status);
}
