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

@QuarkusTest
class QueryShowcaseServiceTest {

    @Inject
    QueryShowcaseService queryService;

    @Inject
    Morphium morphium;

    @BeforeEach
    void setUp() {
        morphium.dropCollection(Employee.class);
        queryService.seedData();
    }

    @Test
    void shouldFindAllEmployees() {
        List<Employee> all = queryService.findAll();
        assertThat(all).hasSize(15);
    }

    @Test
    void shouldFindByDepartment() {
        List<Employee> engineers = queryService.findByDepartment("Engineering");
        assertThat(engineers).isNotEmpty();
        assertThat(engineers).allMatch(e -> e.getDepartment().equals("Engineering"));
    }

    @Test
    void shouldFindBySalaryRange() {
        List<Employee> midRange = queryService.findBySalaryRange(60000, 100000);
        assertThat(midRange).isNotEmpty();
        assertThat(midRange).allMatch(e -> e.getSalary() >= 60000 && e.getSalary() <= 100000);
    }

    @Test
    void shouldFindByNamePattern() {
        List<Employee> sNames = queryService.findByNamePattern("^S.*");
        assertThat(sNames).isNotEmpty();
        assertThat(sNames).allMatch(e -> e.getLastName().startsWith("S"));
    }

    @Test
    void shouldFindActive() {
        List<Employee> active = queryService.findActive();
        assertThat(active).isNotEmpty();
        assertThat(active).allMatch(Employee::isActive);
    }

    @Test
    void shouldGetDistinctDepartments() {
        List<String> departments = queryService.distinctDepartments();
        assertThat(departments).containsExactlyInAnyOrder("Engineering", "Marketing", "Sales", "HR");
    }

    @Test
    void shouldFindPaginated() {
        List<Employee> page1 = queryService.findPaginated(0, 5, "last_name", 1);
        assertThat(page1).hasSize(5);

        List<Employee> page2 = queryService.findPaginated(1, 5, "last_name", 1);
        assertThat(page2).hasSize(5);

        // Pages should be different
        assertThat(page1.get(0).getLastName()).isNotEqualTo(page2.get(0).getLastName());
    }

    @Test
    void shouldCount() {
        assertThat(queryService.count()).isEqualTo(15);
    }

    @Test
    void shouldCountByDepartment() {
        long engCount = queryService.countByDepartment("Engineering");
        assertThat(engCount).isGreaterThan(0);
    }
}