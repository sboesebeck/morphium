package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.data.RepositoryMetadata;
import jakarta.data.repository.By;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Find;
import jakarta.data.repository.First;
import jakarta.data.repository.Is;
import jakarta.data.repository.Query;
import jakarta.data.restrict.Restriction;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import de.caluga.morphium.Morphium;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-019: a repository with a Jakarta Data 1.1 feature that the {@code @Find}, {@code @Delete} and {@code @Query} methods do not implement is refused when
 * the proxy is created, not at the first call. {@code Is}, {@code First} and {@code Restriction} are stand-ins in the test sources: the build uses the 1.0 API,
 * the check works on names.
 */
@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("test")
class UnsupportedParameterStylesTest {

    @Autowired
    Morphium morphium;

    interface WithIs extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
        @Find
        List<TestEntity> cheap(@By("priority") @Is(Object.class) int min);
    }

    interface WithRestriction extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
        @Find
        List<TestEntity> matching(Restriction<TestEntity> restriction);
    }

    interface WithFirstOnQuery extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
        @First(3)
        @Query("WHERE status = :status")
        List<TestEntity> top(@jakarta.data.repository.Param("status") String status);
    }

    interface WithIsOnDelete extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
        @Delete
        long deleteCheap(@By("priority") @Is(Object.class) int max);
    }

    interface Fine extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
        @Find
        List<TestEntity> byStatus(@By("status") String status);
    }

    private void create(Class<?> repository) {
        new MorphiumRepositoryInvocationHandler(morphium, new RepositoryMetadata(TestEntity.class, de.caluga.morphium.driver.MorphiumId.class, "id"), repository);
    }

    @Test
    void anIsParameterOnFindOrDeleteRefusesTheRepository() {
        IllegalStateException find = assertThrows(IllegalStateException.class, () -> create(WithIs.class));
        assertTrue(find.getMessage().contains("WithIs.cheap") && find.getMessage().contains("@Is"), find.getMessage());

        IllegalStateException delete = assertThrows(IllegalStateException.class, () -> create(WithIsOnDelete.class));
        assertTrue(delete.getMessage().contains("WithIsOnDelete.deleteCheap"), delete.getMessage());
    }

    @Test
    void aRestrictionParameterRefusesTheRepository() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> create(WithRestriction.class));
        assertTrue(e.getMessage().contains("WithRestriction.matching") && e.getMessage().contains("Restriction"), e.getMessage());
    }

    @Test
    void firstOnAQueryRefusesTheRepository() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> create(WithFirstOnQuery.class));
        assertTrue(e.getMessage().contains("@First"), e.getMessage());
    }

    @Test
    void aRepositoryWithoutThemIsFine() {
        assertDoesNotThrow(() -> create(Fine.class));
    }
}
