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

    // ------------------------------------------------------------------ MOR-021: @AtomicUpdate is checked when the repository is created, too

    interface AtomicUnknownField extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
        @de.caluga.morphium.data.AtomicUpdate(set = @de.caluga.morphium.data.Assign(field = "nope", value = "1"))
        void bad();
    }

    interface AtomicCollection extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
        @de.caluga.morphium.data.AtomicUpdate(set = @de.caluga.morphium.data.Assign(field = "status", value = "'x'"))
        List<TestEntity> bad();
    }

    interface AtomicUnknownParam extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
        @de.caluga.morphium.data.AtomicUpdate(where = "name = :ghost", set = @de.caluga.morphium.data.Assign(field = "status", value = "'x'"))
        void bad(@jakarta.data.repository.Param("name") String name);
    }

    @Test
    void aWrongAtomicUpdateRefusesTheRepository() {
        IllegalStateException field = assertThrows(IllegalStateException.class, () -> create(AtomicUnknownField.class));
        assertTrue(field.getMessage().contains("AtomicUnknownField.bad") && field.getMessage().contains("nope"), field.getMessage());

        IllegalStateException type = assertThrows(IllegalStateException.class, () -> create(AtomicCollection.class));
        assertTrue(type.getMessage().contains("AtomicCollection.bad") && type.getMessage().contains("not supported"), type.getMessage());

        IllegalStateException param = assertThrows(IllegalStateException.class, () -> create(AtomicUnknownParam.class));
        assertTrue(param.getMessage().contains(":ghost"), param.getMessage());
    }

    // ------------------------------------------------------------------ MOR-026: the Jakarta Data TCK repositories extend BasicRepository, not CrudRepository

    interface OnlyBasic extends jakarta.data.repository.BasicRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
    }

    interface OnlyData extends jakarta.data.repository.DataRepository<TestEntity, de.caluga.morphium.driver.MorphiumId> {
    }

    @Test
    void entityAndIdAreResolvedForEveryJakartaDataRepositoryBase() {
        assertTrue(MorphiumRepositoryFactoryBean.resolveTypeArguments(OnlyBasic.class)[0] == TestEntity.class);
        assertTrue(MorphiumRepositoryFactoryBean.resolveTypeArguments(OnlyData.class)[0] == TestEntity.class);
        assertTrue(MorphiumRepositoryFactoryBean.resolveTypeArguments(OnlyBasic.class)[1] == de.caluga.morphium.driver.MorphiumId.class);
    }
}
