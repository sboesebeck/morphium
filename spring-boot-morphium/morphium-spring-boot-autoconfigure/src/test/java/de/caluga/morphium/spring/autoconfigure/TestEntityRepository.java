package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.data.MorphiumRepository;
import de.caluga.morphium.driver.MorphiumId;
import jakarta.data.Sort;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.data.repository.By;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Find;
import jakarta.data.repository.Query;
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

    // --- Regression coverage: dynamic Sort/PageRequest on a derived query (review finding 1) ---

    /** A dynamic {@link Sort} argument must actually reach the query, not be dropped. */
    List<TestEntity> findByStatus(String status, Sort<TestEntity> sort);

    /** A {@link Page} return type requires the paging-aware bridge overload. */
    Page<TestEntity> findByStatus(String status, PageRequest pageRequest);

    // --- Regression coverage: @Delete with a numeric return type (review finding 2) ---

    /** Jakarta Data permits void/int/long here; the numeric variants return the delete count. */
    @Delete
    long deleteCountedByStatus(@By("status") String status);

    // --- Regression coverage: CompletionStage on @Query and @Find (review finding 3) ---

    /** Must resolve to the async JDQL bridge and yield a LIST, not a single entity. */
    @Query("WHERE status = :status")
    CompletionStage<List<TestEntity>> queryByStatusAsync(@jakarta.data.repository.Param("status") String status);

    /** Must resolve to the async find bridge and yield a LIST, not a single entity. */
    @Find
    CompletionStage<List<TestEntity>> findAsyncByStatus(@By("status") String status);

    // --- Ordinal parameters in @Query (MOR-024) ---

    /** ?1 and ?2 are the first and the second method parameter, in any order of use. */
    @Query("WHERE priority >= ?2 AND status = ?1 ORDER BY priority ASC")
    List<TestEntity> queryOrdinal(String status, int minCount);

    /** A Sort parameter is not counted: ?1 is the status. */
    @Query("WHERE status = ?1")
    List<TestEntity> queryOrdinalSorted(String status, Sort<TestEntity> sort);

    // --- Regression coverage: default method dispatch (review finding 4) ---

    /**
     * A default method composes other repository calls and must run as written. Deliberately
     * named {@code countBy...} to also prove the default check wins over derived-query parsing.
     */
    default long countByStatusViaDefaultMethod(String status) {
        return findByStatus(status).size();
    }
}
