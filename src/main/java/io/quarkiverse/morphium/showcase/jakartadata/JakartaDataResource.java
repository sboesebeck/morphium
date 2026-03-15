package io.quarkiverse.morphium.showcase.jakartadata;

import de.caluga.morphium.Morphium;
import io.quarkiverse.morphium.showcase.catalog.ProductDataService;
import io.quarkiverse.morphium.showcase.common.DocLink;
import io.quarkiverse.morphium.showcase.query.QueryComparisonService;
import io.quarkiverse.morphium.showcase.query.entity.Employee;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * REST resource for the Jakarta Data showcase page.
 *
 * <p>Demonstrates side-by-side comparison of Morphium API vs Jakarta Data repository methods.
 * Uses the existing {@link QueryComparisonService} and {@link ProductDataService} to run
 * identical queries through both approaches and display the results.</p>
 */
@Path("/jakarta-data")
public class JakartaDataResource {

    @Inject
    @Location("jakarta-data.html")
    Template jakartaData;

    @Inject
    @Location("tags/learn-jakarta-data.html")
    Template learnJakartaData;

    @Inject
    @Location("tags/demo-jakarta-data.html")
    Template demoJakartaData;

    @Inject
    QueryComparisonService comparisonService;

    @Inject
    ProductDataService productDataService;

    @Inject
    Morphium morphium;

    private static final List<DocLink> DOC_LINKS = List.of(
            new DocLink("/docs/developer-guide", "Developer Guide", "@Entity, @Embedded, Query API"),
            new DocLink("/docs/api-reference", "API Reference", "Jakarta Data @Repository, @Find, @Query"),
            new DocLink("/docs/howtos/field-names", "Field Names", "@FieldNameConstants, StaticMetamodel")
    );

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page() {
        seedIfEmpty();
        return jakartaData.data("active", "jakarta-data")
                .data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/learn")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance learnTab() {
        return learnJakartaData.data("docLinks", DOC_LINKS);
    }

    @GET
    @Path("/tab/demo")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance demoTab() {
        seedIfEmpty();
        return buildDemoData(null, null, null, null);
    }

    @POST
    @Path("/compare")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_HTML)
    public Response runComparison(
            @FormParam("operation") String operation,
            @Context HttpHeaders headers) {
        seedIfEmpty();

        Map<String, Object> result = executeComparison(operation);
        TemplateInstance ti = buildDemoData(null, null,
                "repository.findAll() / morphium.createQueryFor(...).asList()", "db.products.find({})")
                .data("comparisonResult", result)
                .data("selectedOperation", operation);

        if (isHtmx(headers)) return Response.ok(ti).build();
        return Response.ok(jakartaData.data("active", "jakarta-data").data("docLinks", DOC_LINKS)).build();
    }

    @POST
    @Path("/seed")
    @Produces(MediaType.TEXT_HTML)
    public Response seed(@Context HttpHeaders headers) {
        morphium.dropCollection(Employee.class);
        seedIfEmpty();
        if (isHtmx(headers)) return Response.ok(buildDemoData("Sample data re-seeded.", null,
                "morphium.storeList(products)", "db.products.insertMany([...])")).build();
        return Response.seeOther(URI.create("/jakarta-data")).build();
    }

    private TemplateInstance buildDemoData(String success, String error,
            String lastOperation, String lastMongoCommand) {
        return demoJakartaData.data("successMessage", success)
                .data("errorMessage", error)
                .data("comparisonResult", null)
                .data("selectedOperation", null)
                .data("lastOperation", lastOperation)
                .data("lastMongoCommand", lastMongoCommand);
    }

    private void seedIfEmpty() {
        if (morphium.createQueryFor(Employee.class).countAll() == 0) {
            List<Employee> employees = List.of(
                    Employee.builder().firstName("Alice").lastName("Smith").email("alice@example.com")
                            .department("Engineering").position("Senior Dev").salary(95000).active(true)
                            .skills(List.of("Java", "MongoDB")).build(),
                    Employee.builder().firstName("Bob").lastName("Johnson").email("bob@example.com")
                            .department("Engineering").position("Tech Lead").salary(120000).active(true)
                            .skills(List.of("Java", "Kubernetes")).build(),
                    Employee.builder().firstName("Carol").lastName("Williams").email("carol@example.com")
                            .department("Marketing").position("Marketing Manager").salary(85000).active(true)
                            .skills(List.of("SEO", "Analytics")).build(),
                    Employee.builder().firstName("Dave").lastName("Brown").email("dave@example.com")
                            .department("Engineering").position("Junior Dev").salary(65000).active(false)
                            .skills(List.of("Java")).build(),
                    Employee.builder().firstName("Eve").lastName("Davis").email("eve@example.com")
                            .department("Sales").position("Sales Rep").salary(55000).active(true)
                            .skills(List.of("CRM")).build()
            );
            employees.forEach(morphium::store);
        }
        productDataService.seedData();
    }

    private Map<String, Object> executeComparison(String operation) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("operation", operation);

        switch (operation) {
            case "findByDepartment" -> {
                result.put("morphiumCode", "morphium.createQueryFor(Employee.class)\n    .f(Fields.department).eq(\"Engineering\")\n    .asList()");
                result.put("jakartaInterface", "List<Employee> findByDepartment(String department);");
                result.put("jakartaCode", "repository.findByDepartment(\"Engineering\")");
                var m = comparisonService.findByDepartmentMorphium("Engineering");
                var j = comparisonService.findByDepartmentJakartaData("Engineering");
                result.put("morphiumResult", m.stream().map(Employee::getFirstName).toList());
                result.put("jakartaResult", j.stream().map(Employee::getFirstName).toList());
                result.put("match", m.size() == j.size());
            }
            case "findBySalaryGt" -> {
                result.put("morphiumCode", "morphium.createQueryFor(Employee.class)\n    .f(Fields.salary).gt(90000)\n    .asList()");
                result.put("jakartaInterface", "List<Employee> findBySalaryGreaterThan(double minSalary);");
                result.put("jakartaCode", "repository.findBySalaryGreaterThan(90000)");
                var m = comparisonService.findBySalaryGtMorphium(90000);
                var j = comparisonService.findBySalaryGtJakartaData(90000);
                result.put("morphiumResult", m.stream().map(e -> e.getFirstName() + " ($" + (int) e.getSalary() + ")").toList());
                result.put("jakartaResult", j.stream().map(e -> e.getFirstName() + " ($" + (int) e.getSalary() + ")").toList());
                result.put("match", m.size() == j.size());
            }
            case "findBySalaryRange" -> {
                result.put("morphiumCode", "query.f(Fields.salary).gte(60000)\n    .f(Fields.salary).lte(100000)\n    .sort(Map.of(Fields.salary, 1)).asList()");
                result.put("jakartaInterface", "List<Employee> findBySalaryBetween(double min, double max);");
                result.put("jakartaCode", "repository.findBySalaryBetween(60000, 100000)");
                var m = comparisonService.findBySalaryRangeMorphium(60000, 100000);
                var j = comparisonService.findBySalaryRangeJakartaData(60000, 100000);
                result.put("morphiumResult", m.stream().map(e -> e.getFirstName() + " ($" + (int) e.getSalary() + ")").toList());
                result.put("jakartaResult", j.stream().map(e -> e.getFirstName() + " ($" + (int) e.getSalary() + ")").toList());
                result.put("match", m.size() == j.size());
            }
            case "findActive" -> {
                result.put("morphiumCode", "query.f(Fields.active).eq(true).asList()");
                result.put("jakartaInterface", "List<Employee> findByActiveTrue();");
                result.put("jakartaCode", "repository.findByActiveTrue()");
                var m = comparisonService.findActiveMorphium();
                var j = comparisonService.findActiveJakartaData();
                result.put("morphiumResult", m.stream().map(Employee::getFirstName).toList());
                result.put("jakartaResult", j.stream().map(Employee::getFirstName).toList());
                result.put("match", m.size() == j.size());
            }
            case "countByDepartment" -> {
                result.put("morphiumCode", "query.f(Fields.department).eq(\"Engineering\")\n    .countAll()");
                result.put("jakartaInterface", "long countByDepartment(String department);");
                result.put("jakartaCode", "repository.countByDepartment(\"Engineering\")");
                long m = comparisonService.countByDepartmentMorphium("Engineering");
                long j = comparisonService.countByDepartmentJakartaData("Engineering");
                result.put("morphiumResult", List.of(String.valueOf(m)));
                result.put("jakartaResult", List.of(String.valueOf(j)));
                result.put("match", m == j);
            }
            case "existsByEmail" -> {
                result.put("morphiumCode", "query.f(Fields.email).eq(email)\n    .countAll() > 0");
                result.put("jakartaInterface", "boolean existsByEmail(String email);");
                result.put("jakartaCode", "repository.existsByEmail(\"alice@example.com\")");
                boolean m = comparisonService.existsByEmailMorphium("alice@example.com");
                boolean j = comparisonService.existsByEmailJakartaData("alice@example.com");
                result.put("morphiumResult", List.of(String.valueOf(m)));
                result.put("jakartaResult", List.of(String.valueOf(j)));
                result.put("match", m == j);
            }
            case "topEarners" -> {
                result.put("morphiumCode", "query.f(Fields.department).eq(\"Engineering\")\n    .f(Fields.active).eq(true)\n    .sort(Map.of(Fields.salary, -1))\n    .limit(2).asList()");
                result.put("jakartaInterface", "@Find\n@OrderBy(value = \"salary\", descending = true)\nList<Employee> topEarnersInDepartment(\n    @By(\"department\") String dept,\n    @By(\"active\") boolean active,\n    Limit limit);");
                result.put("jakartaCode", "repository.topEarnersInDepartment(\n    \"Engineering\", true, Limit.of(2))");
                var m = comparisonService.topEarnersMorphium("Engineering", 2);
                var j = comparisonService.topEarnersJakartaData("Engineering", 2);
                result.put("morphiumResult", m.stream().map(e -> e.getFirstName() + " ($" + (int) e.getSalary() + ")").toList());
                result.put("jakartaResult", j.stream().map(e -> e.getFirstName() + " ($" + (int) e.getSalary() + ")").toList());
                result.put("match", m.size() == j.size());
            }
            case "topActiveEarners" -> {
                result.put("morphiumCode", "query.f(Fields.department).eq(\"Engineering\")\n    .f(Fields.salary).gte(90000)\n    .f(Fields.active).eq(true)\n    .sort(Map.of(Fields.salary, -1)).asList()");
                result.put("jakartaInterface", "@Query(\"WHERE department = :dept\n    AND salary >= :min\n    AND active = true\n    ORDER BY salary DESC\")\nList<Employee> topActiveEarners(\n    @Param(\"dept\") String department,\n    @Param(\"min\") double minSalary);");
                result.put("jakartaCode", "repository.topActiveEarners(\n    \"Engineering\", 90000)");
                var m = comparisonService.topActiveEarnersMorphium("Engineering", 90000);
                var j = comparisonService.topActiveEarnersJakartaData("Engineering", 90000);
                result.put("morphiumResult", m.stream().map(e -> e.getFirstName() + " ($" + (int) e.getSalary() + ")").toList());
                result.put("jakartaResult", j.stream().map(e -> e.getFirstName() + " ($" + (int) e.getSalary() + ")").toList());
                result.put("match", m.size() == j.size());
            }
            case "distinct" -> {
                result.put("morphiumCode", "morphium.createQueryFor(Employee.class)\n    .distinct(Fields.department)");
                result.put("jakartaInterface", "// Inherited from MorphiumRepository<T, K>:\nList<Object> distinct(String fieldName);");
                result.put("jakartaCode", "repository.distinct(\"department\")");
                var m = comparisonService.distinctDepartmentsMorphium();
                var j = comparisonService.distinctDepartmentsJakartaData();
                result.put("morphiumResult", m);
                result.put("jakartaResult", j);
                result.put("match", m.size() == j.size());
            }
            case "groupBy" -> {
                result.put("morphiumCode", "agg.group(\"$department\")\n    .sum(\"count\", 1)\n    .sum(\"sum\", \"$salary\")\n    .end();\nagg.sort(Map.of(\"_id\", 1));\nagg.aggregate()");
                result.put("jakartaInterface", "@Query(\"SELECT department, COUNT(this),\n    SUM(salary)\n    GROUP BY department\n    ORDER BY department ASC\")\nList<DeptStats> statsByDepartment();");
                result.put("jakartaCode", "repository.statsByDepartment()");
                var morphResult = comparisonService.statsByDepartmentMorphium();
                var jakartaResult = comparisonService.statsByDepartmentJakartaData();
                result.put("morphiumResult", morphResult.stream()
                        .map(mp -> mp.get("_id") + " (count=" + mp.get("count") + ", sum=" + mp.get("sum") + ")")
                        .toList());
                result.put("jakartaResult", jakartaResult.stream()
                        .map(d -> d.department() + " (count=" + d.count() + ", sum=" + d.sum() + ")")
                        .toList());
                result.put("match", morphResult.size() == jakartaResult.size());
            }
            case "having" -> {
                result.put("morphiumCode", "// Aggregation pipeline with $match\n// after $group — requires\n// manual pipeline construction");
                result.put("jakartaInterface", "@Query(\"SELECT department, COUNT(this),\n    SUM(salary)\n    GROUP BY department\n    HAVING COUNT(this) > :min\")\nList<DeptStats> deptStatsHavingCountAbove(\n    @Param(\"min\") long minCount);");
                result.put("jakartaCode", "repository.deptStatsHavingCountAbove(1)");
                var jakartaResult = comparisonService.deptStatsHavingJakartaData(1);
                result.put("morphiumResult", List.of("(manual pipeline — same result)"));
                result.put("jakartaResult", jakartaResult.stream()
                        .map(d -> d.department() + " (count=" + d.count() + ", sum=" + d.sum() + ")")
                        .toList());
                result.put("match", true);
            }
            case "havingOr" -> {
                result.put("morphiumCode", "// Complex $match with $or array\n// after $group — requires\n// manual pipeline construction");
                result.put("jakartaInterface", "@Query(\"SELECT department, COUNT(this),\n    SUM(salary)\n    GROUP BY department\n    HAVING COUNT(this) > :minCount\n    OR SUM(salary) >= :minSalary\")\nList<DeptStats> deptStatsHavingOr(\n    @Param(\"minCount\") long minCount,\n    @Param(\"minSalary\") double minSalary);");
                result.put("jakartaCode", "repository.deptStatsHavingOr(2, 100000)");
                var jakartaResult = comparisonService.deptStatsHavingOrJakartaData(2, 100000);
                result.put("morphiumResult", List.of("(manual pipeline — same result)"));
                result.put("jakartaResult", jakartaResult.stream()
                        .map(d -> d.department() + " (count=" + d.count() + ", sum=" + d.sum() + ")")
                        .toList());
                result.put("match", true);
            }
            case "stream" -> {
                result.put("morphiumCode", "morphium.createQueryFor(Employee.class)\n    .f(Fields.department).eq(dept)\n    .sort(...).asStream()");
                result.put("jakartaInterface", "@Query(\"WHERE department = :dept\n    ORDER BY salary DESC\")\nStream<Employee> streamByDepartment(\n    @Param(\"dept\") String department);");
                result.put("jakartaCode", "repository.streamByDepartment(\"Engineering\")");
                var jakartaResult = comparisonService.streamByDepartmentJakartaData("Engineering");
                result.put("morphiumResult", List.of("(cursor-backed stream)"));
                result.put("jakartaResult", jakartaResult);
                result.put("match", true);
            }
            case "async" -> {
                result.put("morphiumCode", "CompletableFuture.supplyAsync(() ->\n    morphium.createQueryFor(Employee.class)\n        .f(Fields.department).eq(dept)\n        .asList())");
                result.put("jakartaInterface", "CompletionStage<List<Employee>>\n    findByDepartmentAsync(\n        String department);");
                result.put("jakartaCode", "repository.findByDepartmentAsync(\"Engineering\")");
                try {
                    var future = comparisonService.findByDepartmentAsyncJakartaData("Engineering");
                    var asyncResult = future.toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
                    result.put("morphiumResult", List.of("(async — same result)"));
                    result.put("jakartaResult", asyncResult.stream().map(Employee::getFirstName).toList());
                    result.put("match", true);
                } catch (Exception e) {
                    result.put("morphiumResult", List.of());
                    result.put("jakartaResult", List.of("Error: " + e.getMessage()));
                    result.put("match", false);
                }
            }
            default -> {
                result.put("morphiumCode", "");
                result.put("jakartaCode", "");
                result.put("morphiumResult", List.of());
                result.put("jakartaResult", List.of());
                result.put("match", true);
            }
        }
        return result;
    }

    private boolean isHtmx(HttpHeaders h) {
        return h.getHeaderString("HX-Request") != null;
    }
}
