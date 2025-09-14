package de.caluga.test.mongo.suite.aggregationStages;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("aggregation")
public class GraphLookupTest extends MorphiumTestBase {


    @Test
    public void graphLookup() throws Exception {
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
}
