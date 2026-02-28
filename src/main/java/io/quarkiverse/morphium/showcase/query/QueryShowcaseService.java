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

@ApplicationScoped
public class QueryShowcaseService {

    @Inject
    Morphium morphium;

    public List<Employee> findAll() {
        return morphium.createQueryFor(Employee.class)
                .sort(Map.of(Employee.Fields.lastName, 1))
                .asList();
    }

    public List<Employee> findByDepartment(String dept) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .asList();
    }

    public List<Employee> findBySalaryRange(double min, double max) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.salary).gte(min)
                .f(Employee.Fields.salary).lte(max)
                .sort(Map.of(Employee.Fields.salary, 1))
                .asList();
    }

    public List<Employee> findByNamePattern(String regex) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.lastName).matches(regex)
                .asList();
    }

    public List<Employee> findBySkills(List<String> skills) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.skills).in(skills)
                .asList();
    }

    public List<Employee> findActive() {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.active).eq(true)
                .asList();
    }

    public List<Employee> findWithSalaryAbove(double min) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.salary).gt(min)
                .sort(Map.of(Employee.Fields.salary, -1))
                .asList();
    }

    public List<Employee> findExcludingDepartments(List<String> depts) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).nin(depts)
                .asList();
    }

    public List<Employee> findWithEmail() {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.email).exists()
                .asList();
    }

    public long countByDepartment(String dept) {
        return morphium.createQueryFor(Employee.class)
                .f(Employee.Fields.department).eq(dept)
                .countAll();
    }

    @SuppressWarnings("unchecked")
    public List<String> distinctDepartments() {
        return (List<String>) (List<?>) morphium.createQueryFor(Employee.class)
                .distinct(Employee.Fields.department);
    }

    public List<Employee> findPaginated(int page, int size, String sortField, int sortDir) {
        return morphium.createQueryFor(Employee.class)
                .sort(Map.of(sortField, sortDir))
                .skip(page * size)
                .limit(size)
                .asList();
    }

    public long count() {
        return morphium.createQueryFor(Employee.class).countAll();
    }

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

        morphium.storeList(employees);
    }
}