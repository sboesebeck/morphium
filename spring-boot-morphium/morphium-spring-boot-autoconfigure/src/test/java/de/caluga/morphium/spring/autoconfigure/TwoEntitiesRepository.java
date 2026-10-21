package de.caluga.morphium.spring.autoconfigure;

import jakarta.data.repository.Insert;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;

import java.util.List;
import java.util.Optional;

/** MOR-077: a repository without a primary entity; every method works on the entity that it names, takes or returns */
@Repository
public interface TwoEntitiesRepository {

    @Insert
    TestEntity add(TestEntity entity);

    @Insert
    OtherEntity[] add(OtherEntity... others);

    @Query("DELETE FROM OtherEntity")
    long removeOthers();

    @Query("DELETE FROM TestEntity WHERE status = :status")
    long removeTests(@Param("status") String status);

    @Query("UPDATE OtherEntity SET size = size + ?2 WHERE label = ?1")
    long grow(String label, int by);

    @Query("UPDATE OtherEntity SET size = size / ?2 WHERE label = ?1")
    boolean shrink(String label, int by);

    @Query("DELETE FROM OtherEntity WHERE label = ?1")
    boolean removeLabel(String label);

    @Query("WHERE label = ?1")
    Optional<OtherEntity> otherByLabel(String label);

    @Query("WHERE status = :status ORDER BY priority")
    List<TestEntity> testsByStatus(@Param("status") String status);

    @Query("SELECT label FROM OtherEntity WHERE size > ?1 ORDER BY label")
    List<String> labels(int minSize);
}
