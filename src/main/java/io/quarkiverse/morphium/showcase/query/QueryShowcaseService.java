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
package io.quarkiverse.morphium.showcase.query;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.query.entity.Employee;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Service demonstrating the full breadth of Morphium's Query API for MongoDB.
 *
 * <p>This is the most comprehensive query showcase in the project, covering virtually all
 * common query operators and patterns available in Morphium's fluent Query API. Each method
 * demonstrates a different query capability and maps directly to a MongoDB query operator.</p>
 *
 * <h3>Query API overview:</h3>
 * <p>Morphium queries follow this general pattern:</p>
 * <pre>{@code
 *   morphium.createQueryFor(EntityClass.class)  // 1. Create a query for a collection
 *           .f("fieldName").operator(value)       // 2. Add filter conditions
 *           .sort(Map.of("field", direction))     // 3. Optional: sorting
 *           .skip(n).limit(m)                     // 4. Optional: pagination
 *           .asList();                            // 5. Execute and return results
 * }</pre>
 *
 * <h3>Query operators demonstrated:</h3>
 * <table>
 *   <tr><th>Morphium Method</th><th>MongoDB Operator</th><th>Description</th></tr>
 *   <tr><td>{@code eq(value)}</td><td>{@code $eq}</td><td>Exact equality match</td></tr>
 *   <tr><td>{@code gte(value)}</td><td>{@code $gte}</td><td>Greater than or equal</td></tr>
 *   <tr><td>{@code lte(value)}</td><td>{@code $lte}</td><td>Less than or equal</td></tr>
 *   <tr><td>{@code gt(value)}</td><td>{@code $gt}</td><td>Strictly greater than</td></tr>
 *   <tr><td>{@code matches(regex)}</td><td>{@code $regex}</td><td>Regular expression match</td></tr>
 *   <tr><td>{@code in(list)}</td><td>{@code $in}</td><td>Value is in the given list</td></tr>
 *   <tr><td>{@code nin(list)}</td><td>{@code $nin}</td><td>Value is NOT in the given list</td></tr>
 *   <tr><td>{@code exists()}</td><td>{@code $exists: true}</td><td>Field exists and is not null</td></tr>
 * </table>
 *
 * <h3>Additional features demonstrated:</h3>
 * <ul>
 *   <li>{@code sort()} -- server-side sorting with ascending/descending direction</li>
 *   <li>{@code skip()} and {@code limit()} -- server-side pagination</li>
 *   <li>{@code countAll()} -- server-side document counting</li>
 *   <li>{@code distinct()} -- server-side distinct value extraction</li>
 * </ul>
 *
 * @see de.caluga.morphium.query.Query
 * @see Employee
 */
@ApplicationScoped
public class QueryShowcaseService {

    @Inject
    Morphium morphium;

    /**
     * Retrieves all employees, sorted alphabetically by last name (ascending).
     *
     * <p>The sort direction is specified as an integer: {@code 1} for ascending,
     * {@code -1} for descending. This convention matches MongoDB's native sort specification.
     * The sort benefits from the compound index {@code {lastName, firstName}} defined on
     * the {@link Employee} entity.</p>
     *
     * @return all employees sorted by last name A-Z
     */
    public List<Employee> findAll() {
        // sort() with value 1 = ascending order (A to Z)
        // This query benefits from the compound index {lastName, firstName} on Employee
        return morphium.createQueryFor(Employee.class)
                .sort(Map.of(Employee.Fields.lastName, 1))
                .asList();
    }

    /**
     * Finds employees by department using an equality filter.
     *
     * <p><b>f().eq()</b> is the most fundamental Morphium query operation. It translates to
     * MongoDB's equality match: {@code {"department": "Engineering"}}. The {@code f()} method
     * selects the field, and {@code eq()} applies the equality constraint.</p>
     *
     * <p>This query benefits from the {@code @Index} annotation on the {@code department} field,
     * which creates a single-field index that MongoDB uses to efficiently locate matching documents.</p>
     *
     * @param dept the department name to filter by
     * @return employees belonging to the specified department
     */
    public List<Employee> findByDepartment(String dept) {
        // f() selects the "department" field; eq() matches exact value
        // MongoDB query: { "department": dept }
        // Uses the @Index on department for efficient lookups
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .asList();
    }

    /**
     * Finds employees with a salary within the given range (inclusive on both ends).
     *
     * <p>Demonstrates Morphium's <b>range query operators</b>:</p>
     * <ul>
     *   <li>{@code gte(min)} -- greater than or equal ({@code $gte}) sets the lower bound</li>
     *   <li>{@code lte(max)} -- less than or equal ({@code $lte}) sets the upper bound</li>
     * </ul>
     *
     * <p>When multiple {@code f().operator()} calls are chained on the same query, they are
     * combined with AND logic. This translates to:
     * {@code {"salary": {"$gte": min, "$lte": max}}}</p>
     *
     * <p>Results are sorted by salary ascending so the lowest-paid employees appear first.</p>
     *
     * @param min minimum salary (inclusive)
     * @param max maximum salary (inclusive)
     * @return employees with salaries in the specified range, sorted by salary ascending
     */
    public List<Employee> findBySalaryRange(double min, double max) {
        // Chaining two conditions on the same field creates a range query.
        // gte() = greater-than-or-equal ($gte), lte() = less-than-or-equal ($lte)
        // MongoDB query: { "salary": { "$gte": min, "$lte": max } }
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.salary).gte(min)
                .f(Employee.Fields.salary).lte(max)
                .sort(Map.of(Employee.Fields.salary, 1))
                .asList();
    }

    /**
     * Finds employees whose last name matches a regular expression pattern.
     *
     * <p><b>matches(regex)</b> translates to MongoDB's {@code $regex} operator, enabling
     * pattern-based text matching. Examples:</p>
     * <ul>
     *   <li>{@code "^M"} -- last names starting with "M" (Mueller, etc.)</li>
     *   <li>{@code "er$"} -- last names ending with "er" (Weber, Fischer, etc.)</li>
     *   <li>{@code ".*sch.*"} -- last names containing "sch" (Schmidt, Fischer, Schaefer)</li>
     * </ul>
     *
     * <p><b>Performance note:</b> Regex queries that start with a wildcard (e.g., {@code ".*pattern"})
     * cannot use indexes efficiently and will trigger a collection scan. Prefix-anchored patterns
     * (e.g., {@code "^prefix"}) can leverage indexes.</p>
     *
     * @param regex the regular expression pattern to match against last names
     * @return employees whose last name matches the pattern
     */
    public List<Employee> findByNamePattern(String regex) {
        // matches() applies a regex filter on the field, equivalent to MongoDB's $regex operator.
        // MongoDB query: { "lastName": { "$regex": regex } }
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.lastName).matches(regex)
                .asList();
    }

    /**
     * Finds employees who have any of the specified skills.
     *
     * <p><b>in(list)</b> translates to MongoDB's {@code $in} operator. When applied to an
     * array field like {@code skills}, it matches documents where the array contains at
     * least one value from the provided list. This is equivalent to an OR condition across
     * the values.</p>
     *
     * <p>Example: {@code findBySkills(List.of("Java", "Python"))} finds all employees who
     * have "Java" OR "Python" (or both) in their skills list.</p>
     *
     * <p>MongoDB query: {@code {"skills": {"$in": ["Java", "Python"]}}}</p>
     *
     * @param skills the list of skill names to search for
     * @return employees who have at least one of the specified skills
     */
    public List<Employee> findBySkills(List<String> skills) {
        // in() checks if the field value is in the provided list.
        // For array fields, $in matches if ANY element of the array is in the list.
        // MongoDB query: { "skills": { "$in": skills } }
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.skills).in(skills)
                .asList();
    }

    /**
     * Finds all active employees.
     *
     * <p>Demonstrates a simple boolean equality query. {@code f("active").eq(true)}
     * translates to {@code {"active": true}} in MongoDB.</p>
     *
     * @return all employees where {@code active == true}
     */
    public List<Employee> findActive() {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.active).eq(true)
                .asList();
    }

    /**
     * Finds employees with a salary strictly above the given minimum, sorted descending.
     *
     * <p><b>gt(value)</b> is the strict "greater than" operator ({@code $gt}), as opposed to
     * {@code gte()} which includes the boundary value. Results are sorted by salary descending
     * (highest paid first) using sort direction {@code -1}.</p>
     *
     * @param min the minimum salary (exclusive -- employees earning exactly this amount are excluded)
     * @return employees earning more than the specified amount, sorted highest to lowest
     */
    public List<Employee> findWithSalaryAbove(double min) {
        // gt() = strictly greater than ($gt), unlike gte() which includes the boundary
        // sort with -1 = descending order (highest salary first)
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.salary).gt(min)
                .sort(Map.of(Employee.Fields.salary, -1))
                .asList();
    }

    /**
     * Finds employees NOT in any of the specified departments.
     *
     * <p><b>nin(list)</b> translates to MongoDB's {@code $nin} (not in) operator. It matches
     * documents where the field value is NOT in the provided list. This is the negation of
     * {@code in()}.</p>
     *
     * <p>Example: {@code findExcludingDepartments(List.of("Sales", "HR"))} returns all
     * employees who are in Engineering, Marketing, or any department other than Sales/HR.</p>
     *
     * @param depts the list of department names to exclude
     * @return employees whose department is NOT in the exclusion list
     */
    public List<Employee> findExcludingDepartments(List<String> depts) {
        // nin() = "not in" ($nin) -- matches documents where the field value is NOT in the list
        // MongoDB query: { "department": { "$nin": depts } }
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).nin(depts)
                .asList();
    }

    /**
     * Finds employees who have an email address set (field exists and is non-null).
     *
     * <p><b>exists()</b> translates to MongoDB's {@code $exists: true} operator. It matches
     * documents where the specified field is present in the BSON document. Note that this
     * also matches documents where the field is explicitly set to {@code null} -- to exclude
     * nulls, you would need to add an additional {@code .ne(null)} condition.</p>
     *
     * <p>In this dataset, some employees have a {@code null} email. Since Morphium does not
     * store null fields by default, those documents simply lack the "email" field, so
     * {@code exists()} effectively filters them out.</p>
     *
     * @return employees who have an email field in their MongoDB document
     */
    public List<Employee> findWithEmail() {
        // exists() checks if the field is present in the document ($exists: true)
        // MongoDB query: { "email": { "$exists": true } }
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.email).exists()
                .asList();
    }

    /**
     * Counts employees in a specific department using a server-side count operation.
     *
     * <p>{@code countAll()} sends a {@code countDocuments()} command to MongoDB which counts
     * matching documents on the server without transferring any document data. Combined with
     * a field filter, this is efficient for dashboard-style aggregation counts.</p>
     *
     * @param dept the department to count employees in
     * @return the number of employees in the specified department
     */
    public long countByDepartment(String dept) {
        // countAll() executes a server-side count -- no documents are transferred
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .countAll();
    }

    /**
     * Returns a list of distinct department names across all employees.
     *
     * <p><b>distinct(fieldName)</b> executes MongoDB's {@code distinct} command, which returns
     * all unique values for the specified field across the collection. This runs entirely on the
     * server and is much more efficient than loading all documents and extracting unique values
     * in Java.</p>
     *
     * <p>Morphium's {@code distinct()} returns a raw {@code List<Object>}, so the unchecked
     * cast to {@code List<String>} is necessary. The {@code @SuppressWarnings("unchecked")}
     * annotation acknowledges this type-safety gap.</p>
     *
     * <p>This operation benefits from the {@code @Index} annotation on the {@code department}
     * field, as MongoDB can serve the distinct values directly from the index without scanning
     * the full collection (a "covered query").</p>
     *
     * @return a list of unique department names
     */
    @SuppressWarnings("unchecked")
    public List<String> distinctDepartments() {
        // distinct() runs MongoDB's distinct command on the server.
        // Returns all unique values for the "department" field.
        // The @Index on department allows MongoDB to serve this from the index directly.
        return (List<String>) (List<?>) morphium.createQueryFor(Employee.class)
                .distinct(Employee.Fields.department);
    }

    /**
     * Returns a paginated subset of employees with sorting.
     *
     * <p>Demonstrates Morphium's <b>server-side pagination</b> using {@code skip()} and
     * {@code limit()}, which map directly to MongoDB's cursor methods:</p>
     * <ul>
     *   <li>{@code skip(page * size)} -- skips the first N documents. For page 0 with size 5,
     *       skip is 0; for page 1, skip is 5; for page 2, skip is 10; etc.</li>
     *   <li>{@code limit(size)} -- restricts the result to at most {@code size} documents.</li>
     * </ul>
     *
     * <p><b>Important:</b> Skip-based pagination has linear performance degradation for large
     * offsets because MongoDB still needs to scan and discard the skipped documents. For large
     * collections, consider range-based pagination (e.g., using {@code _id > lastSeenId}) or
     * Morphium's iterator-based approach with {@code query.asIterable()}.</p>
     *
     * @param page      zero-based page number
     * @param size      number of documents per page
     * @param sortField the field name to sort by (e.g., "lastName", "salary")
     * @param sortDir   sort direction: 1 for ascending, -1 for descending
     * @return the requested page of employees
     */
    public List<Employee> findPaginated(int page, int size, String sortField, int sortDir) {
        // skip() + limit() implement offset-based pagination on the server.
        // skip(page * size) calculates the number of documents to skip for the current page.
        // limit(size) restricts the number of returned documents to the page size.
        return morphium.createQueryFor(Employee.class)
                .sort(Map.of(sortField, sortDir))
                .skip(page * size)
                .limit(size)
                .asList();
    }

    /**
     * Counts all employees in the collection.
     *
     * @return the total number of employee documents
     */
    public long count() {
        return morphium.createQueryFor(Employee.class).countAll();
    }

    /**
     * Seeds the collection with 15 sample employees for demonstration purposes.
     *
     * <p>Uses {@code morphium.storeList()} to insert all employees in a single bulk operation.
     * The guard {@code if (count() > 0) return;} ensures idempotency -- data is only seeded
     * if the collection is empty.</p>
     *
     * <p>The sample data includes:</p>
     * <ul>
     *   <li>Employees across 4 departments (Engineering, Marketing, Sales, HR)</li>
     *   <li>Salary range from 48,000 to 110,000 for range query demos</li>
     *   <li>Some employees with null email to demonstrate {@code exists()} and {@code @IgnoreNullFromDB}</li>
     *   <li>Some inactive employees to demonstrate boolean filtering</li>
     *   <li>Various skills arrays to demonstrate {@code in()} queries</li>
     * </ul>
     */
    public void seedData() {
        if (count() > 0) return;

        List<Employee> employees = List.of(
                Employee.builder()
                        .firstName("Alice").lastName("Mueller").email("alice.mueller@example.com")
                        .department("Engineering").position("Senior Developer").salary(95000)
                        .hireDate(LocalDateTime.of(2019, 3, 15, 0, 0)).skills(List.of("Java", "MongoDB", "Quarkus")).active(true).build(),
                Employee.builder()
                        .firstName("Bob").lastName("Schmidt").email("bob.schmidt@example.com")
                        .department("Engineering").position("Tech Lead").salary(110000)
                        .hireDate(LocalDateTime.of(2017, 7, 1, 0, 0)).skills(List.of("Java", "Kubernetes", "Architecture")).active(true).build(),
                Employee.builder()
                        .firstName("Clara").lastName("Weber").email("clara.weber@example.com")
                        .department("Engineering").position("Junior Developer").salary(60000)
                        .hireDate(LocalDateTime.of(2023, 1, 10, 0, 0)).skills(List.of("Python", "JavaScript", "React")).active(true).build(),
                Employee.builder()
                        .firstName("David").lastName("Fischer").email(null)
                        .department("Engineering").position("DevOps Engineer").salary(88000)
                        .hireDate(LocalDateTime.of(2020, 11, 20, 0, 0)).skills(List.of("Docker", "Kubernetes", "Terraform")).active(true).build(),
                Employee.builder()
                        .firstName("Eva").lastName("Braun").email("eva.braun@example.com")
                        .department("Marketing").position("Marketing Manager").salary(82000)
                        .hireDate(LocalDateTime.of(2018, 5, 5, 0, 0)).skills(List.of("SEO", "Analytics", "Content")).active(true).build(),
                Employee.builder()
                        .firstName("Frank").lastName("Hoffmann").email("frank.hoffmann@example.com")
                        .department("Marketing").position("Content Specialist").salary(55000)
                        .hireDate(LocalDateTime.of(2022, 9, 1, 0, 0)).skills(List.of("Writing", "SEO", "Social Media")).active(true).build(),
                Employee.builder()
                        .firstName("Greta").lastName("Zimmermann").email(null)
                        .department("Marketing").position("Designer").salary(65000)
                        .hireDate(LocalDateTime.of(2021, 4, 15, 0, 0)).skills(List.of("Figma", "Photoshop", "UI/UX")).active(false).build(),
                Employee.builder()
                        .firstName("Hans").lastName("Koch").email("hans.koch@example.com")
                        .department("Sales").position("Sales Director").salary(105000)
                        .hireDate(LocalDateTime.of(2016, 2, 1, 0, 0)).skills(List.of("Negotiation", "CRM", "Leadership")).active(true).build(),
                Employee.builder()
                        .firstName("Ingrid").lastName("Bauer").email("ingrid.bauer@example.com")
                        .department("Sales").position("Account Manager").salary(72000)
                        .hireDate(LocalDateTime.of(2020, 8, 12, 0, 0)).skills(List.of("CRM", "Communication", "Analytics")).active(true).build(),
                Employee.builder()
                        .firstName("Jan").lastName("Richter").email(null)
                        .department("Sales").position("Sales Representative").salary(50000)
                        .hireDate(LocalDateTime.of(2024, 1, 8, 0, 0)).skills(List.of("Communication", "Prospecting")).active(true).build(),
                Employee.builder()
                        .firstName("Katrin").lastName("Wolf").email("katrin.wolf@example.com")
                        .department("Sales").position("Sales Analyst").salary(62000)
                        .hireDate(LocalDateTime.of(2022, 6, 20, 0, 0)).skills(List.of("Excel", "Analytics", "SQL")).active(false).build(),
                Employee.builder()
                        .firstName("Lars").lastName("Schaefer").email("lars.schaefer@example.com")
                        .department("HR").position("HR Director").salary(98000)
                        .hireDate(LocalDateTime.of(2015, 10, 1, 0, 0)).skills(List.of("Recruiting", "Leadership", "Compliance")).active(true).build(),
                Employee.builder()
                        .firstName("Maria").lastName("Neumann").email("maria.neumann@example.com")
                        .department("HR").position("Recruiter").salary(58000)
                        .hireDate(LocalDateTime.of(2021, 3, 22, 0, 0)).skills(List.of("Recruiting", "Interviewing", "Onboarding")).active(true).build(),
                Employee.builder()
                        .firstName("Niklas").lastName("Schwarz").email(null)
                        .department("HR").position("HR Specialist").salary(53000)
                        .hireDate(LocalDateTime.of(2023, 7, 1, 0, 0)).skills(List.of("Payroll", "Benefits", "Compliance")).active(true).build(),
                Employee.builder()
                        .firstName("Olivia").lastName("Krueger").email("olivia.krueger@example.com")
                        .department("HR").position("Training Coordinator").salary(48000)
                        .hireDate(LocalDateTime.of(2024, 2, 14, 0, 0)).skills(List.of("Training", "Development", "Communication")).active(true).build()
        );

        // storeList() persists all 15 employees in a single MongoDB bulk write operation
        morphium.storeList(employees);
    }
}
