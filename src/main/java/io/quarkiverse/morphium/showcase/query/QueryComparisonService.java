package io.quarkiverse.morphium.showcase.query;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.query.entity.Employee;
import jakarta.data.Limit;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;

/**
 * Side-by-side comparison service: executes the same queries using both Morphium API
 * and Jakarta Data, returning identical results.
 *
 * <p>Each method pair demonstrates the same operation in two ways:</p>
 * <ul>
 *   <li>{@code xxxMorphium()} — direct Morphium Query API</li>
 *   <li>{@code xxxJakartaData()} — Jakarta Data repository method</li>
 * </ul>
 *
 * <p>Operations like {@code distinct()} are available through both approaches:
 * the Morphium Query API and {@code MorphiumRepository.distinct()}.</p>
 */
@ApplicationScoped
public class QueryComparisonService {

    @Inject
    Morphium morphium;

    @Inject
    EmployeeRepository repository;

    // ========== Equality ==========

    public List<Employee> findByDepartmentMorphium(String dept) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .asList();
    }

    public List<Employee> findByDepartmentJakartaData(String dept) {
        return repository.findByDepartment(dept);
    }

    // ========== Greater Than ==========

    public List<Employee> findBySalaryGtMorphium(double min) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.salary).gt(min)
                .asList();
    }

    public List<Employee> findBySalaryGtJakartaData(double min) {
        return repository.findBySalaryGreaterThan(min);
    }

    // ========== Between (Range) ==========

    public List<Employee> findBySalaryRangeMorphium(double min, double max) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.salary).gte(min)
                .f(Employee.Fields.salary).lte(max)
                .sort(Map.of(Employee.Fields.salary, 1))
                .asList();
    }

    public List<Employee> findBySalaryRangeJakartaData(double min, double max) {
        return repository.findBySalaryBetween(min, max);
    }

    // ========== Pattern Matching (LIKE / Regex) ==========

    public List<Employee> searchByNameMorphium(String pattern) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.lastName).matches("(?i)" + pattern)
                .asList();
    }

    public List<Employee> searchByNameJakartaData(String pattern) {
        // JDQL LIKE uses SQL-style % wildcards instead of regex
        return repository.searchByName("%" + pattern + "%");
    }

    // ========== Boolean Filter ==========

    public List<Employee> findActiveMorphium() {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.active).eq(true)
                .asList();
    }

    public List<Employee> findActiveJakartaData() {
        return repository.findByActiveTrue();
    }

    // ========== Composite (AND) ==========

    public List<Employee> findByDeptAndActiveMorphium(String dept) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .f(Employee.Fields.active).eq(true)
                .asList();
    }

    public List<Employee> findByDeptAndActiveJakartaData(String dept) {
        return repository.findByDepartmentAndActiveTrue(dept);
    }

    // ========== Count ==========

    public long countByDepartmentMorphium(String dept) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .countAll();
    }

    public long countByDepartmentJakartaData(String dept) {
        return repository.countByDepartment(dept);
    }

    // ========== Exists ==========

    public boolean existsByEmailMorphium(String email) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.email).eq(email)
                .countAll() > 0;
    }

    public boolean existsByEmailJakartaData(String email) {
        return repository.existsByEmail(email);
    }

    // ========== Pagination ==========

    public List<Employee> findPageMorphium(String dept, int page, int size) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .sort(Map.of(Employee.Fields.salary, -1))
                .skip((page - 1) * size)
                .limit(size)
                .asList();
    }

    public Page<Employee> findPageJakartaData(String dept, int page, int size) {
        // Using Sort.desc() directly — alternatively, Employee_.salary.desc() with the Metamodel
        return repository.findByDepartment(dept,
                PageRequest.ofPage(page, size, true),
                Order.by(Sort.desc("salary")));
    }

    // ========== Top N with @Find / @OrderBy ==========

    public List<Employee> topEarnersMorphium(String dept, int limit) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .f(Employee.Fields.active).eq(true)
                .sort(Map.of(Employee.Fields.salary, -1))
                .limit(limit)
                .asList();
    }

    public List<Employee> topEarnersJakartaData(String dept, int limit) {
        return repository.topEarnersInDepartment(dept, true, Limit.of(limit));
    }

    // ========== JDQL with complex filter ==========

    public List<Employee> topActiveEarnersMorphium(String dept, double minSalary) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .f(Employee.Fields.salary).gte(minSalary)
                .f(Employee.Fields.active).eq(true)
                .sort(Map.of(Employee.Fields.salary, -1))
                .asList();
    }

    public List<Employee> topActiveEarnersJakartaData(String dept, double minSalary) {
        return repository.topActiveEarners(dept, minSalary);
    }

    // ========== Distinct ==========

    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<String> distinctDepartmentsMorphium() {
        List result = morphium.createQueryFor(Employee.class)
                .distinct(Employee.Fields.department);
        return result;
    }

    @SuppressWarnings("unchecked")
    public List<String> distinctDepartmentsJakartaData() {
        // MorphiumRepository.distinct() — the escape hatch for distinct queries
        return (List<String>) (List<?>) repository.distinct(Employee.Fields.department);
    }

    // ========== Morphium-only: Atomic Update ($inc) ==========

    public void giveRaiseMorphium(String dept, double amount) {
        // Jakarta Data has no atomic field update support
        var query = morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept);
        morphium.inc(query, Employee.Fields.salary, amount);
    }

    // ========== Morphium-only: $set / $unset ==========

    public void updatePositionMorphium(String dept, String newPosition) {
        // Jakarta Data has no partial field update support
        morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .set(Employee.Fields.position, newPosition, false, true, null);
    }
}
