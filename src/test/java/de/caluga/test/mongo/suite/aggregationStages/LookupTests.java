package de.caluga.test.mongo.suite.aggregationStages;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("aggregation")
public class LookupTests extends MorphiumTestBase {

    @Test
    public void singleEqualityJoinTest() throws Exception {
        morphium.dropCollection(Order.class);
        morphium.dropCollection(Inventory.class);
        morphium.store(new Order(1, "almonds", 12.0, 2));
        morphium.store(new Order(2, "pecans", 20.0, 1));
        morphium.store(new Order(3, null, null, null));
        morphium.store(new Order(4, "peanuts", 8.0, 12));
        morphium.store(new Inventory(1, "almonds", "Product 1", 120));
        morphium.store(new Inventory(2, "bread", "Product 2", 80));
        morphium.store(new Inventory(3, "cashews", "Product 3", 60));
        morphium.store(new Inventory(4, "pecans", "Product 4", 70));
        morphium.store(new Inventory(5, null, "incomplete", null));
        Thread.sleep(100);
        Aggregator<Order, Map> agg = morphium.createAggregator(Order.class, Map.class);
        List<Map> lst = agg.lookup(morphium.getMapper().getCollectionName(Inventory.class), "item", "sku", "inventory_docs", null, null).aggregate();

        for (Map m : lst) {
            log.info("Map: " + m.toString());
            assertNotNull(m.get("inventory_docs"));
            ;

            if (m.get("_id").equals(4)) {
                assert(((List) m.get("inventory_docs")).size() == 0);
            } else {
                assert(((List) m.get("inventory_docs")).size() == 1);
            }
        }
    }


    @Test
    public void multipleConditionAndPipelines() throws Exception {
        morphium.dropCollection(Order.class);
        morphium.dropCollection(Inventory.class);
        Thread.sleep(100);
        morphium.store(new Order(1, "almonds", 12.0, 20));
        morphium.store(new Order(2, "pecans", 20.0, 18));
        morphium.store(new Order(3, "cashews", 11.0, 12));
        morphium.store(new Order(4, "peanuts", 12.0, 14));
        morphium.store(new Order(5, "bread", 12.0, 84));
        morphium.store(new Inventory(1, "almonds", "Product 1", "Warehouse A", 120));
        morphium.store(new Inventory(2, "almonds", "Product 1", "Warehouse C", 120));
        morphium.store(new Inventory(3, "bread", "Product 2", "Warehouse A", 80));
        morphium.store(new Inventory(4, "cashews", "Product 3", "Warehouse B", 60));
        morphium.store(new Inventory(5, "pecans", "Product 4", "Warehouse B", 15));
        morphium.store(new Inventory(6, "pecans", "Product 6", "Warehouse C", 70));
        morphium.store(new Inventory(7, "peanuts", "peanuts", "Warehouse B", 10));
        morphium.store(new Inventory(8, "peanuts", "peanuts!", "Warehouse A", 120));
        Thread.sleep(150);
        Aggregator<Order, Map> agg = morphium.createAggregator(Order.class, Map.class);
        List<Expr> pipeline = new ArrayList<>();
        pipeline.add(Expr.match(Expr.expr(
            Expr.and(
                Expr.eq(Expr.field("sku"), Expr.field("$$order_item")),
                Expr.gte(Expr.field("$instock"), Expr.field("$$order_qty"))
            ))));
        pipeline.add(Expr.project(UtilsMap.of("sku", Expr.intExpr(0), "_id", Expr.intExpr(0))));
        agg.lookup("inventory", null, null, "stock_data", pipeline, UtilsMap.of("order_item", Expr.field("item"), "order_qty", Expr.field("quantity")));
        List<Map<String, Object >> result = agg.aggregateMap();
        for (Map<String, Object> m : result) {
            log.info(m.toString());

            if (m.get("_id").equals(1)) {
                //should be two possible warehouses
                assert(((List) m.get("stock_data")).size() == 2);
            } else if (m.get("_id").equals(5)) {
                assert(((List) m.get("stock_data")).size() == 0);  //not available
            } else {
                assert(((List) m.get("stock_data")).size() == 1);
            }
        }

        log.info("done");
    }

    @Entity
    public static class Order {
        @Id
        public Integer id;
        public String item;
        public Double price;
        public Integer quantity;


        public Order(Integer id, String item, Double price, Integer quantity) {
            this.id = id;
            this.item = item;
            this.price = price;
            this.quantity = quantity;
        }
    }

    @Entity
    public static class Inventory {
        @Id
        public Integer id;
        public String sku;
        public String description;
        public String warehouse;
        public Integer instock;

        public Inventory(Integer id, String sku, String description, Integer instock) {
            this.id = id;
            this.sku = sku;
            this.description = description;
            this.instock = instock;
        }

        public Inventory(Integer id, String sku, String description, String warehouse, Integer instock) {
            this.id = id;
            this.sku = sku;
            this.description = description;
            this.instock = instock;
            this.warehouse = warehouse;
        }
    }


}
