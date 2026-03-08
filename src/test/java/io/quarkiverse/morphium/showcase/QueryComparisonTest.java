package io.quarkiverse.morphium.showcase;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.query.QueryComparisonService;
import io.quarkiverse.morphium.showcase.query.EmployeeRepository;
import io.quarkiverse.morphium.showcase.query.entity.Employee;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that Morphium API and Jakarta Data produce identical results.
 *
 * <p>Each test runs the same query through both approaches and asserts that the results match.
 * This proves that Jakarta Data is a faithful abstraction over Morphium's query engine.</p>
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryComparisonTest {

    @Inject
    QueryComparisonService service;

    @Inject
    EmployeeRepository repository;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(Employee.class);
        seedEmployees();
    }

    private void seedEmployees() {
        List<Employee> employees = List.of(
                Employee.builder().firstName("Alice").lastName("Smith").email("alice@example.com")
                        .department("Engineering").position("Senior Dev").salary(95000).active(true)
                        .hireDate(LocalDateTime.of(2020, 1, 15, 0, 0)).skills(List.of("Java", "MongoDB")).build(),
                Employee.builder().firstName("Bob").lastName("Johnson").email("bob@example.com")
                        .department("Engineering").position("Tech Lead").salary(120000).active(true)
                        .hireDate(LocalDateTime.of(2018, 6, 1, 0, 0)).skills(List.of("Java", "Kubernetes")).build(),
                Employee.builder().firstName("Carol").lastName("Williams").email("carol@example.com")
                        .department("Marketing").position("Marketing Manager").salary(85000).active(true)
                        .hireDate(LocalDateTime.of(2019, 3, 10, 0, 0)).skills(List.of("SEO", "Analytics")).build(),
                Employee.builder().firstName("Dave").lastName("Brown").email("dave@example.com")
                        .department("Engineering").position("Junior Dev").salary(65000).active(false)
                        .hireDate(LocalDateTime.of(2022, 9, 1, 0, 0)).skills(List.of("Java")).build(),
                Employee.builder().firstName("Eve").lastName("Davis").email("eve@example.com")
                        .department("Sales").position("Sales Rep").salary(55000).active(true)
                        .hireDate(LocalDateTime.of(2021, 7, 20, 0, 0)).skills(List.of("CRM")).build()
        );
        employees.forEach(morphium::store);
    }

    // ---- Equality ----

    @Test
    @Order(1)
    @DisplayName("Comparison: findByDepartment — Morphium vs Jakarta Data")
    void shouldMatchFindByDepartment() {
        var morphiumResult = service.findByDepartmentMorphium("Engineering");
        var jakartaResult = service.findByDepartmentJakartaData("Engineering");

        assertThat(morphiumResult).hasSize(3);
        assertThat(jakartaResult).hasSize(3);
        assertThat(jakartaResult).extracting(Employee::getEmail)
                .containsExactlyInAnyOrderElementsOf(
                        morphiumResult.stream().map(Employee::getEmail).toList());
    }

    // ---- Greater Than ----

    @Test
    @Order(2)
    @DisplayName("Comparison: findBySalaryGreaterThan — Morphium vs Jakarta Data")
    void shouldMatchFindBySalaryGt() {
        var morphiumResult = service.findBySalaryGtMorphium(90000);
        var jakartaResult = service.findBySalaryGtJakartaData(90000);

        assertThat(morphiumResult).hasSize(2); // Alice 95k, Bob 120k
        assertThat(jakartaResult).hasSize(2);
        assertThat(jakartaResult).extracting(Employee::getFirstName)
                .containsExactlyInAnyOrderElementsOf(
                        morphiumResult.stream().map(Employee::getFirstName).toList());
    }

    // ---- Between ----

    @Test
    @Order(3)
    @DisplayName("Comparison: findBySalaryBetween — Morphium vs Jakarta Data")
    void shouldMatchFindBySalaryRange() {
        var morphiumResult = service.findBySalaryRangeMorphium(60000, 100000);
        var jakartaResult = service.findBySalaryRangeJakartaData(60000, 100000);

        assertThat(morphiumResult).hasSize(3); // Alice 95k, Carol 85k, Dave 65k
        assertThat(jakartaResult).hasSize(3);
    }

    // ---- Boolean Filter ----

    @Test
    @Order(4)
    @DisplayName("Comparison: findByActiveTrue — Morphium vs Jakarta Data")
    void shouldMatchFindActive() {
        var morphiumResult = service.findActiveMorphium();
        var jakartaResult = service.findActiveJakartaData();

        assertThat(morphiumResult).hasSize(4); // all except Dave
        assertThat(jakartaResult).hasSize(4);
    }

    // ---- Composite (AND) ----

    @Test
    @Order(5)
    @DisplayName("Comparison: findByDeptAndActive — Morphium vs Jakarta Data")
    void shouldMatchComposite() {
        var morphiumResult = service.findByDeptAndActiveMorphium("Engineering");
        var jakartaResult = service.findByDeptAndActiveJakartaData("Engineering");

        assertThat(morphiumResult).hasSize(2); // Alice, Bob (Dave is inactive)
        assertThat(jakartaResult).hasSize(2);
    }

    // ---- Count ----

    @Test
    @Order(6)
    @DisplayName("Comparison: countByDepartment — Morphium vs Jakarta Data")
    void shouldMatchCount() {
        long morphiumCount = service.countByDepartmentMorphium("Engineering");
        long jakartaCount = service.countByDepartmentJakartaData("Engineering");

        assertThat(morphiumCount).isEqualTo(3);
        assertThat(jakartaCount).isEqualTo(3);
    }

    // ---- Exists ----

    @Test
    @Order(7)
    @DisplayName("Comparison: existsByEmail — Morphium vs Jakarta Data")
    void shouldMatchExists() {
        assertThat(service.existsByEmailMorphium("alice@example.com")).isTrue();
        assertThat(service.existsByEmailJakartaData("alice@example.com")).isTrue();
        assertThat(service.existsByEmailMorphium("nonexistent@example.com")).isFalse();
        assertThat(service.existsByEmailJakartaData("nonexistent@example.com")).isFalse();
    }

    // ---- Top Earners (@Find/@By with Limit) ----

    @Test
    @Order(10)
    @DisplayName("Comparison: topEarners — Morphium vs Jakarta Data (@Find/@OrderBy)")
    void shouldMatchTopEarners() {
        var morphiumResult = service.topEarnersMorphium("Engineering", 2);
        var jakartaResult = service.topEarnersJakartaData("Engineering", 2);

        assertThat(morphiumResult).hasSize(2);
        assertThat(jakartaResult).hasSize(2);
        // Both should be sorted by salary DESC: Bob (120k), Alice (95k)
        assertThat(morphiumResult.get(0).getFirstName()).isEqualTo("Bob");
        assertThat(jakartaResult.get(0).getFirstName()).isEqualTo("Bob");
    }

    // ---- JDQL ----

    @Test
    @Order(20)
    @DisplayName("Comparison: topActiveEarners — Morphium vs Jakarta Data (JDQL)")
    void shouldMatchTopActiveEarners() {
        var morphiumResult = service.topActiveEarnersMorphium("Engineering", 90000);
        var jakartaResult = service.topActiveEarnersJakartaData("Engineering", 90000);

        assertThat(morphiumResult).hasSize(2); // Alice 95k, Bob 120k (Dave is inactive)
        assertThat(jakartaResult).hasSize(2);
    }

    // ---- Morphium-only features ----

    @Test
    @Order(30)
    @DisplayName("Morphium-only: distinct departments")
    void shouldDistinct() {
        var departments = service.distinctDepartmentsMorphium();
        assertThat(departments).containsExactlyInAnyOrder("Engineering", "Marketing", "Sales");
    }

    @Test
    @Order(31)
    @DisplayName("Morphium-only: atomic $inc (salary raise)")
    void shouldInc() {
        double sumBefore = service.findByDepartmentJakartaData("Engineering").stream()
                .mapToDouble(Employee::getSalary).sum();

        service.giveRaiseMorphium("Engineering", 5000);

        // morphium.inc() updates the first matching document (updateOne semantics)
        var engineering = service.findByDepartmentJakartaData("Engineering");
        double sumAfter = engineering.stream().mapToDouble(Employee::getSalary).sum();
        assertThat(sumAfter).isGreaterThan(sumBefore);
        assertThat(engineering).anyMatch(e -> e.getSalary() == 100000.0); // Alice: 95k + 5k
    }

    // ---- Metamodel ----

    @Test
    @Order(40)
    @DisplayName("Metamodel: Employee_ class exists")
    void shouldHaveEmployeeMetamodel() throws Exception {
        Class<?> metamodel = Class.forName("io.quarkiverse.morphium.showcase.query.entity.Employee_");
        assertThat(metamodel).isNotNull();

        var annotation = metamodel.getAnnotation(jakarta.data.metamodel.StaticMetamodel.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.value()).isEqualTo(Employee.class);
    }
}
