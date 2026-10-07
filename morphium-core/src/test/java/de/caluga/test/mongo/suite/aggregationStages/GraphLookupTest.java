package de.caluga.test.mongo.suite.aggregationStages;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

import java.util.HashMap;
import static org.assertj.core.api.Assertions.assertThat;

@Tag("aggregation")
public class GraphLookupTest extends MultiDriverTestBase {


    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void graphLookup(Morphium morphium) throws Exception  {
        morphium.dropCollection(Employee.class);
        Thread.sleep(200);
        morphium.store(new Employee(1, "Dev", null));
        morphium.store(new Employee(2, "Eliot", "Dev"));
        morphium.store(new Employee(3, "Ron", "Eliot"));
        morphium.store(new Employee(4, "Andrew", "Eliot"));
        morphium.store(new Employee(5, "Asya", "Ron"));
        morphium.store(new Employee(6, "Dan", "Andrew"));

        Aggregator<Employee, Map> agg = morphium.createAggregator(Employee.class, Map.class);
        agg.graphLookup(Employee.class, Expr.field("reports_to"), "reports_to", "name", "reportingHierarchy", null, null, null);
        List<Map<String, Object>> map = agg.aggregateMap();

        for (Map<String, Object> m : map) {
            log.info(m.toString());
            assertNotNull(m.get("reportingHierarchy"));
            ;
        }
    }


    /**
     * The scenario documented in docs/howtos/aggregation-examples.md section 7: an integer
     * "reportsTo -&gt; _id" reporting hierarchy combined with a maxDepth cap and a depthField.
     * The manager's own document is never part of the result; the traversal collects the
     * transitive reporting line under it, and maxDepth stops the recursion so deeper members
     * (here Asya, two levels below the seed) are not reached. The assertions avoid pinning the
     * inmem-vs-mongod divergence on whether the input document appears in its own hierarchy.
     */
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void graphLookupIntHierarchyWithMaxDepthAndDepthField(Morphium morphium) throws Exception {
        morphium.dropCollection(ReportingEmployee.class);
        Thread.sleep(200);
        morphium.store(new ReportingEmployee(1, "Dev", null));
        morphium.store(new ReportingEmployee(2, "Eliot", 1));
        morphium.store(new ReportingEmployee(3, "Ron", 2));
        morphium.store(new ReportingEmployee(4, "Andrew", 2));
        morphium.store(new ReportingEmployee(5, "Asya", 3));

        Aggregator<ReportingEmployee, Map> agg = morphium.createAggregator(ReportingEmployee.class, Map.class);
        agg.graphLookup(ReportingEmployee.class, Expr.field("reports_to"), "_id", "reports_to",
                "hierarchy", 1, "depth", null);
        List<Map<String, Object>> map = agg.aggregateMap();

        Map<String, Object> eliotResult = null;
        for (Map<String, Object> m : map) {
            if ("Eliot".equals(m.get("name"))) {
                eliotResult = m;
                break;
            }
        }

        assertThat(eliotResult).as("Eliot should be among the input documents").isNotNull();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hierarchy = (List<Map<String, Object>>) eliotResult.get("hierarchy");
        Map<String, Map<String, Object>> byName = new HashMap<>();
        for (Map<String, Object> e : hierarchy) {
            byName.put((String) e.get("name"), e);
        }

        // the manager's own document is never part of the result
        assertThat(byName).doesNotContainKey("Dev");
        // direct reports of the manager (Eliot's boss = id 1) are found at depth 1
        assertThat(byName).containsKeys("Ron", "Andrew");
        assertThat(((Number) byName.get("Ron").get("depth")).intValue()).isEqualTo(1);
        assertThat(((Number) byName.get("Andrew").get("depth")).intValue()).isEqualTo(1);
        // maxDepth=1 stops the recursion: Asya (deeper) must not be reached
        assertThat(byName).doesNotContainKey("Asya");
    }


    @Entity
    public static class Employee {
        @Id
        public int id;
        public String name;
        public String reportsTo;

        public Employee(int id, String name, String reportsTo) {
            this.id = id;
            this.name = name;
            this.reportsTo = reportsTo;
        }
    }


    @Entity
    public static class ReportingEmployee {
        @Id
        public int id;
        public String name;
        public Integer reportsTo;

        public ReportingEmployee(int id, String name, Integer reportsTo) {
            this.id = id;
            this.name = name;
            this.reportsTo = reportsTo;
        }
    }
}
