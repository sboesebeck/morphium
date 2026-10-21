package de.caluga.morphium.spring.autoconfigure;

import jakarta.data.repository.Delete;
import jakarta.data.repository.Insert;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;
import jakarta.data.repository.Save;
import jakarta.data.repository.Update;

import java.util.List;

/** MOR-077: a repository that extends nothing; its primary entity is the one of its lifecycle methods */
@Repository
public interface LifecycleOnlyRepository {

    @Insert
    TestEntity add(TestEntity entity);

    @Insert
    List<TestEntity> addAll(List<TestEntity> entities);

    @Insert
    TestEntity[] addArray(TestEntity... entities);

    @Insert
    void addQuietly(TestEntity entity);

    @Save
    void saveOne(TestEntity entity);

    @Update
    TestEntity change(TestEntity entity);

    @Update
    boolean changeAll(List<TestEntity> entities);

    @Delete
    void remove(List<TestEntity> entities);

    @Delete
    long removeOne(TestEntity entity);

    long countByStatus(String status);

    List<TestEntity> findByStatus(String status);

    @Query("DELETE FROM TestEntity WHERE status = :status")
    long dropStatus(@Param("status") String status);

    @Query("WHERE name = ?1")
    java.util.Optional<TestEntity> byName(String name);
}
