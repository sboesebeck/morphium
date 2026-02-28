/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.quarkiverse.morphium.showcase;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.query.QueryShowcaseService;
import io.quarkiverse.morphium.showcase.query.entity.Employee;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link QueryShowcaseService} -- demonstrates the breadth of Morphium's
 * query API against the InMemDriver.
 *
 * <h2>How This Test Works</h2>
 * <p>
 * Like the other showcase tests, this class uses {@code @QuarkusTest} to boot the full Quarkus
 * application context. The test-scoped configuration sets:
 * </p>
 * <pre>
 *   quarkus.morphium.database=inmem-test
 *   quarkus.morphium.driver-name=InMemDriver
 *   quarkus.morphium.devservices.enabled=false
 * </pre>
 * <p>
 * This means all queries execute against Morphium's in-memory driver -- no Docker, no network,
 * no Testcontainers. The InMemDriver faithfully supports the query operators demonstrated here:
 * equality, range (gte/lte), regex, boolean filters, distinct, pagination (skip/limit), and
 * sorting.
 * </p>
 *
 * <h2>Test Data Setup</h2>
 * <p>
 * Unlike the other showcase tests that start from an empty collection, this test class calls
 * {@link QueryShowcaseService#seedData()} in {@code @BeforeEach} to populate a realistic
 * dataset of 15 employees across 4 departments. This makes the query tests meaningful because
 * we can verify filters, counts, and pagination against known data.
 * </p>
 *
 * <h2>What This Test Covers</h2>
 * <ul>
 *   <li>{@code .f(field).eq(value)} -- exact match queries</li>
 *   <li>{@code .f(field).gte(min).f(field).lte(max)} -- range queries</li>
 *   <li>{@code .f(field).matches(regex)} -- regex pattern matching ({@code $regex})</li>
 *   <li>{@code .f(field).eq(true)} -- boolean field queries</li>
 *   <li>{@code .distinct(field)} -- retrieving distinct field values</li>
 *   <li>{@code .sort().skip().limit()} -- pagination with sorting</li>
 *   <li>{@code .countAll()} -- document counting, both global and filtered</li>
 * </ul>
 *
 * @see QueryShowcaseService
 * @see de.caluga.morphium.driver.inmem.InMemoryDriver
 */
@QuarkusTest
class QueryShowcaseServiceTest {

    // The QueryShowcaseService exposes various Morphium query patterns for demonstration.
    @Inject
    QueryShowcaseService queryService;

    // Direct Morphium access for collection management during test setup.
    @Inject
    Morphium morphium;

    /**
     * Resets the Employee collection and seeds it with 15 known employee records.
     * <p>
     * The pattern here is: drop the collection first, then call {@code seedData()}.
     * Since {@code seedData()} has an idempotency guard ({@code if (count() > 0) return}),
     * dropping first ensures the data is always freshly inserted. This guarantees that
     * every test method operates on the same known dataset of 15 employees in 4 departments
     * (Engineering, Marketing, Sales, HR).
     * </p>
     */
    @BeforeEach
    void setUp() {
        // Drop the collection to remove all data and indexes
        morphium.dropCollection(Employee.class);

        // Seed the collection with 15 employees (4 Engineering, 3 Marketing, 4 Sales, 4 HR)
        queryService.seedData();
    }

    /**
     * Verifies that {@code findAll()} returns all 15 seeded employees.
     * <p>
     * Under the hood, this is a simple unbounded query:
     * <pre>
     *   morphium.createQueryFor(Employee.class)
     *       .sort(Map.of(Employee.Fields.lastName, 1))
     *       .asList();
     * </pre>
     * The {@code sort(Map.of(field, 1))} sorts ascending (1) by last name. Use -1 for
     * descending order.
     * </p>
     */
    @Test
    void shouldFindAllEmployees() {
        List<Employee> all = queryService.findAll();

        // The seed data contains exactly 15 employees
        assertThat(all).hasSize(15);
    }

    /**
     * Tests filtering by an exact field value using {@code .f(field).eq(value)}.
     * <p>
     * This is the most basic Morphium query pattern. The generated {@code Employee.Fields}
     * inner class provides compile-time-safe field name constants, avoiding typos in
     * string-based field references.
     * <pre>
     *   morphium.createQueryFor(Employee.class)
     *       .f(Employee.Fields.department).eq("Engineering")
     *       .asList();
     * </pre>
     * </p>
     */
    @Test
    void shouldFindByDepartment() {
        List<Employee> engineers = queryService.findByDepartment("Engineering");

        // The seed data has 4 Engineering employees
        assertThat(engineers).isNotEmpty();

        // Verify ALL returned employees actually belong to Engineering (no false positives)
        assertThat(engineers).allMatch(e -> e.getDepartment().equals("Engineering"));
    }

    /**
     * Tests a salary range query using {@code .gte()} and {@code .lte()} operators.
     * <p>
     * Morphium's query builder supports chaining multiple conditions on the same field.
     * When you call {@code .f(field).gte(min)} followed by {@code .f(field).lte(max)},
     * both conditions are combined with AND semantics in the generated MongoDB query:
     * <pre>
     *   { "salary": { "$gte": 60000, "$lte": 100000 } }
     * </pre>
     * </p>
     */
    @Test
    void shouldFindBySalaryRange() {
        // Find employees with salary between 60,000 and 100,000
        List<Employee> midRange = queryService.findBySalaryRange(60000, 100000);

        assertThat(midRange).isNotEmpty();

        // Verify every returned employee's salary is within the requested range
        assertThat(midRange).allMatch(e -> e.getSalary() >= 60000 && e.getSalary() <= 100000);
    }

    /**
     * Tests regex-based queries using Morphium's {@code .matches()} operator.
     * <p>
     * The {@code .matches(regex)} method translates to MongoDB's {@code $regex} operator.
     * Here we use {@code "^S.*"} to find all employees whose last name starts with "S".
     * The InMemDriver supports Java regex syntax for these queries.
     * <pre>
     *   morphium.createQueryFor(Employee.class)
     *       .f(Employee.Fields.lastName).matches("^S.*")
     *       .asList();
     * </pre>
     * </p>
     */
    @Test
    void shouldFindByNamePattern() {
        // Regex: names starting with "S" (Schmidt, Schwarz, Schaefer)
        List<Employee> sNames = queryService.findByNamePattern("^S.*");

        assertThat(sNames).isNotEmpty();

        // Verify every result actually has a last name starting with "S"
        assertThat(sNames).allMatch(e -> e.getLastName().startsWith("S"));
    }

    /**
     * Tests filtering on a boolean field using {@code .f(field).eq(true)}.
     * <p>
     * The seed data includes both active and inactive employees. This test confirms that
     * the boolean equality filter works correctly and only returns active employees.
     * </p>
     */
    @Test
    void shouldFindActive() {
        List<Employee> active = queryService.findActive();

        // Most of the 15 seed employees are active (only 2 are inactive)
        assertThat(active).isNotEmpty();

        // Verify only active employees are returned
        assertThat(active).allMatch(Employee::isActive);
    }

    /**
     * Tests the {@code distinct()} query operation for retrieving unique field values.
     * <p>
     * Morphium's {@code distinct(field)} maps to MongoDB's {@code distinct} command. It
     * returns a list of all unique values for the specified field across all documents
     * in the collection:
     * <pre>
     *   morphium.createQueryFor(Employee.class)
     *       .distinct(Employee.Fields.department);
     * </pre>
     * Note: The return type is {@code List<Object>}, so the service casts it to
     * {@code List<String>} since we know departments are strings.
     * </p>
     */
    @Test
    void shouldGetDistinctDepartments() {
        List<String> departments = queryService.distinctDepartments();

        // The seed data spans exactly 4 departments
        assertThat(departments).containsExactlyInAnyOrder("Engineering", "Marketing", "Sales", "HR");
    }

    /**
     * Tests pagination using Morphium's {@code .skip()} and {@code .limit()} methods.
     * <p>
     * Pagination in Morphium follows the standard MongoDB cursor pattern:
     * <pre>
     *   morphium.createQueryFor(Employee.class)
     *       .sort(Map.of("last_name", 1))   // sort ascending by last name
     *       .skip(page * size)               // skip previous pages
     *       .limit(size)                     // limit to page size
     *       .asList();
     * </pre>
     * Note that {@code sort()} uses the MongoDB field name ({@code "last_name"}, the
     * snake_case version) rather than the Java field name. This is because Morphium maps
     * camelCase Java fields to snake_case MongoDB fields by default.
     * </p>
     */
    @Test
    void shouldFindPaginated() {
        // Fetch page 0 (first 5 employees, sorted by last name ascending)
        List<Employee> page1 = queryService.findPaginated(0, 5, "last_name", 1);
        assertThat(page1).hasSize(5);

        // Fetch page 1 (next 5 employees)
        List<Employee> page2 = queryService.findPaginated(1, 5, "last_name", 1);
        assertThat(page2).hasSize(5);

        // The two pages should contain different employees (verified by comparing first entries)
        assertThat(page1.get(0).getLastName()).isNotEqualTo(page2.get(0).getLastName());
    }

    /**
     * Tests the global {@code countAll()} method which counts all documents in the collection.
     * <p>
     * {@code morphium.createQueryFor(Employee.class).countAll()} is equivalent to
     * MongoDB's {@code db.collection.countDocuments({})}. This is useful for sanity-checking
     * test data setup.
     * </p>
     */
    @Test
    void shouldCount() {
        // Seed data contains exactly 15 employees
        assertThat(queryService.count()).isEqualTo(15);
    }

    /**
     * Tests a filtered count using {@code countAll()} with a query condition.
     * <p>
     * When you add filters before calling {@code countAll()}, Morphium generates a
     * filtered count query:
     * <pre>
     *   morphium.createQueryFor(Employee.class)
     *       .f(Employee.Fields.department).eq("Engineering")
     *       .countAll();
     * </pre>
     * This is equivalent to {@code db.collection.countDocuments({ department: "Engineering" })}.
     * </p>
     */
    @Test
    void shouldCountByDepartment() {
        // Count only Engineering employees
        long engCount = queryService.countByDepartment("Engineering");

        // The seed data has 4 Engineering employees, so the count should be positive
        assertThat(engCount).isGreaterThan(0);
    }
}
