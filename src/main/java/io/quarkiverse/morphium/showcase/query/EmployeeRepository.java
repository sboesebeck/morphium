package io.quarkiverse.morphium.showcase.query;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.quarkus.data.MorphiumRepository;
import io.quarkiverse.morphium.showcase.query.entity.Employee;
import jakarta.data.Limit;
import jakarta.data.Order;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.data.repository.Find;
import jakarta.data.repository.OrderBy;
import jakarta.data.repository.Param;
import jakarta.data.repository.Query;
import jakarta.data.repository.Repository;
import jakarta.data.repository.By;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

/**
 * Jakarta Data repository for {@link Employee} — comprehensive query operator comparison.
 *
 * <p>This repository mirrors every query operation from {@link QueryShowcaseService} to
 * demonstrate the Jakarta Data equivalent for each Morphium query operator.</p>
 *
 * <h3>Operator Mapping: Morphium → Jakarta Data</h3>
 * <table>
 *   <tr><th>Morphium</th><th>Jakarta Data (Query Derivation)</th><th>Jakarta Data (JDQL)</th></tr>
 *   <tr><td>{@code .f("dept").eq(val)}</td><td>{@code findByDepartment(val)}</td><td>{@code WHERE department = :val}</td></tr>
 *   <tr><td>{@code .f("salary").gt(val)}</td><td>{@code findBySalaryGreaterThan(val)}</td><td>{@code WHERE salary > :val}</td></tr>
 *   <tr><td>{@code .f("salary").gte(min).f("salary").lte(max)}</td><td>{@code findBySalaryBetween(min, max)}</td><td>{@code WHERE salary BETWEEN :min AND :max}</td></tr>
 *   <tr><td>{@code .f("name").matches(pattern)}</td><td>{@code findByLastNameLike(pattern)}</td><td>{@code WHERE lastName LIKE :pattern}</td></tr>
 *   <tr><td>{@code .f("active").eq(true)}</td><td>{@code findByActiveTrue()}</td><td>{@code WHERE active = true}</td></tr>
 *   <tr><td>{@code .countAll()}</td><td>{@code countByDepartment(dept)}</td><td>{@code WHERE department = :dept} (long return)</td></tr>
 * </table>
 *
 * <h3>Morphium ORM features that work transparently through this repository</h3>
 * <ul>
 *   <li>{@code @Version} — optimistic locking on save/update</li>
 *   <li>{@code @CreationTime} / {@code @LastChange} — auto-timestamps</li>
 *   <li>{@code @PreStore} / {@code @PostLoad} — lifecycle callbacks</li>
 *   <li>{@code @Cache} / {@code @WriteBuffer} — read cache and write batching</li>
 * </ul>
 *
 * <h3>MorphiumRepository escape hatch</h3>
 * <ul>
 *   <li>{@code repository.distinct("department")} — distinct values via MorphiumRepository</li>
 *   <li>{@code repository.morphium().inc(query, "salary", amount)} — atomic field updates</li>
 *   <li>{@code repository.query().f("position").eq(val)} — typed Morphium Query</li>
 *   <li>Aggregation pipeline ($group, $project, $unwind) via {@code repository.morphium()}</li>
 * </ul>
 */
@Repository
public interface EmployeeRepository extends MorphiumRepository<Employee, MorphiumId> {

    // ---- Equality ----
    /** Morphium: {@code .f("department").eq(dept).asList()} */
    List<Employee> findByDepartment(String department);

    // ---- Comparison operators ----
    /** Morphium: {@code .f("salary").gt(min).asList()} */
    List<Employee> findBySalaryGreaterThan(double minSalary);

    /** Morphium: {@code .f("salary").gte(min).asList()} */
    List<Employee> findBySalaryGreaterThanEqual(double minSalary);

    /** Morphium: {@code .f("salary").lt(max).asList()} */
    List<Employee> findBySalaryLessThan(double maxSalary);

    /** Morphium: {@code .f("salary").lte(max).asList()} */
    List<Employee> findBySalaryLessThanEqual(double maxSalary);

    // ---- Range ----
    /** Morphium: {@code .f("salary").gte(min).f("salary").lte(max).asList()} */
    List<Employee> findBySalaryBetween(double min, double max);

    // ---- Pattern matching ----
    /** Morphium: {@code .f("lastName").matches(pattern).asList()} */
    List<Employee> findByLastNameLike(String pattern);

    // ---- Boolean ----
    /** Morphium: {@code .f("active").eq(true).asList()} */
    List<Employee> findByActiveTrue();

    /** Morphium: {@code .f("active").eq(false).asList()} */
    List<Employee> findByActiveFalse();

    // ---- Composite (AND) ----
    /** Morphium: {@code .f("department").eq(dept).f("active").eq(true).asList()} */
    List<Employee> findByDepartmentAndActiveTrue(String department);

    // ---- Count ----
    /** Morphium: {@code .f("department").eq(dept).countAll()} */
    long countByDepartment(String department);

    // ---- Exists ----
    /** Morphium: {@code .f("email").eq(email).countAll() > 0} */
    boolean existsByEmail(String email);

    // ---- Pagination & Sorting ----
    Page<Employee> findByDepartment(String department, PageRequest pageRequest, Order<Employee> order);

    // ---- @Find / @By ----

    /** Find active employees in a department, sorted by salary descending */
    @Find
    @OrderBy(value = "salary", descending = true)
    List<Employee> topEarnersInDepartment(@By("department") String dept, @By("active") boolean active, Limit limit);

    // ---- @Query / JDQL ----

    /** Top earners by department with min salary filter */
    @Query("WHERE department = :dept AND salary >= :min AND active = true ORDER BY salary DESC")
    List<Employee> topActiveEarners(@Param("dept") String department, @Param("min") double minSalary);

    /** Search employees by name pattern across first or last name */
    @Query("WHERE lastName LIKE :pattern OR firstName LIKE :pattern ORDER BY lastName ASC")
    List<Employee> searchByName(@Param("pattern") String namePattern);

    /** Count active employees in a department */
    @Query("WHERE department = :dept AND active = true")
    long countActiveInDepartment(@Param("dept") String department);

    // ---- GROUP BY + Aggregation ----

    /** Headcount and total salary per department */
    @Query("SELECT department, COUNT(this), SUM(salary) GROUP BY department ORDER BY department ASC")
    List<DeptStats> statsByDepartment();

    /** Only departments with headcount above a threshold */
    @Query("SELECT department, COUNT(this), SUM(salary) GROUP BY department HAVING COUNT(this) > :min ORDER BY department ASC")
    List<DeptStats> deptStatsHavingCountAbove(@Param("min") long minCount);

    /** Departments where headcount OR total salary exceed thresholds */
    @Query("SELECT department, COUNT(this), SUM(salary) GROUP BY department HAVING COUNT(this) > :minCount OR SUM(salary) >= :minSalary")
    List<DeptStats> deptStatsHavingOr(@Param("minCount") long minCount, @Param("minSalary") double minSalary);

    // ---- Stream ----

    /** Cursor-backed lazy stream for memory-efficient processing */
    @Query("WHERE department = :dept ORDER BY salary DESC")
    Stream<Employee> streamByDepartment(@Param("dept") String department);

    // ---- Async ----

    /** Non-blocking query returning a CompletionStage */
    CompletionStage<List<Employee>> findByDepartmentAsync(String department);

    // ---- Record return types for GROUP BY ----

    record DeptStats(String department, long count, double sum) {}
}
