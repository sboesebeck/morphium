package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.data.MorphiumRepository;
import de.caluga.morphium.driver.MorphiumId;
import jakarta.data.repository.By;
import jakarta.data.repository.Find;
import jakarta.data.repository.Repository;

import java.util.List;
import java.util.concurrent.CompletionStage;

@Repository
public interface TestEntityRepository extends MorphiumRepository<TestEntity, MorphiumId> {

    List<TestEntity> findByStatus(String status);

    List<TestEntity> findByStatusAndPriority(String status, int priority);

    long countByStatus(String status);

    CompletionStage<List<TestEntity>> findByStatusAsync(String status);

    @Find
    List<TestEntity> byStatus(@By("status") String status);
}
