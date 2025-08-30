# Aggregation Examples

These examples use `Aggregator<T,R>` and `Expr` to build pipelines fluently.

Imports used
```java
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
```

1) Grouping with sums and averages
```java
Aggregator<Order, Map> agg = morphium.createAggregator(Order.class, Map.class);
agg.match(morphium.createQueryFor(Order.class).f("status").eq("OPEN"));
agg.group("$customerId").sum("total", "$amount").avg("avgAmount", "$amount").count("count").end();
agg.sort("-total");
List<Map> perCustomer = agg.aggregate();
```

2) Project computed fields and unwind arrays
```java
Aggregator<Order, Map> agg = morphium.createAggregator(Order.class, Map.class);
agg.unwind("$items");
agg.project(Expr.fields(
  Expr.include("_id", 0),
  Expr.field("sku", Expr.field("items.sku")),
  Expr.field("revenue", Expr.multiply(Expr.field("items.price"), Expr.field("items.qty")))
));
agg.group("$sku").sum("total", "$revenue").count("sold", 1).end();
List<Map> perSku = agg.aggregate();
```

3) Lookup (join) with another collection
```java
Aggregator<Order, Map> agg = morphium.createAggregator(Order.class, Map.class);
agg.lookup("customers", "customerId", "_id", "customer", null, null); // simple local/foreign join
agg.unwind("$customer");
agg.project("_id", "amount", "customer.name");
List<Map> ordersWithCustomer = agg.aggregate();
```

4) Bucketing numeric values
```java
var boundaries = List.of(Expr.intExpr(0), Expr.intExpr(50), Expr.intExpr(100), Expr.intExpr(200));
Aggregator<Order, Map> agg = morphium.createAggregator(Order.class, Map.class);
agg.bucket(Expr.field("amount"), boundaries, null, Map.of(
  "count", Expr.sum(1),
  "sum", Expr.sum(Expr.field("amount"))
));
List<Map> byAmountBucket = agg.aggregate();
```

5) Automatic bucketing
```java
Aggregator<Order, Map> agg = morphium.createAggregator(Order.class, Map.class);
agg.bucketAuto(Expr.field("amount"), 5, Map.of("sum", Expr.sum(Expr.field("amount"))), Aggregator.BucketGranularity.POWERSOF2);
List<Map> buckets = agg.aggregate();
```

6) Sort by count and top‑N
```java
Aggregator<Order, Map> agg = morphium.createAggregator(Order.class, Map.class);
agg.sortByCount(Expr.field("customerId"));
agg.limit(10);
List<Map> topCustomers = agg.aggregate();
```

7) Graph lookup (hierarchical relations)
```java
Aggregator<Employee, Map> agg = morphium.createAggregator(Employee.class, Map.class);
agg.graphLookup(
  Employee.class,
  Expr.field("$reportsTo"), // start with
  "_id",                   // connectFrom
  "reportsTo",             // connectTo
  "hierarchy",             // output array
  5,                        // max depth
  "depth",                 // depth field
  null                      // optional match filter
);
List<Map> hierarchy = agg.aggregate();
```

8) Faceted search
```java
var base = morphium.createAggregator(Order.class, Map.class)
  .match(morphium.createQueryFor(Order.class).f("status").eq("OPEN"));

var byCustomer = morphium.createAggregator(Order.class, Map.class)
  .group("$customerId").sum("total", "$amount").end();

var byDay = morphium.createAggregator(Order.class, Map.class)
  .project(Expr.fields(
    Expr.field("day", Expr.dateToString("%Y-%m-%d", Expr.field("created"))),
    Expr.include("amount")
  ))
  .group("$day").sum("total", "$amount").end();

base.facet(Map.of("byCustomer", byCustomer, "byDay", byDay));
List<Map> facets = base.aggregate();
```

9) Typed result projection
```java
public class SalesSummary {
  private String customerId;
  private BigDecimal total;
  private long count;
  // getters/setters
}

Aggregator<Order, SalesSummary> agg = morphium.createAggregator(Order.class, SalesSummary.class);
agg.group("$customerId").sum("total", "$amount").count("count").end();
List<SalesSummary> result = agg.aggregate();
```

Tips
- Use `Expr` helpers to build expressions (e.g., `Expr.field`, `Expr.sum`, `Expr.divide`, `Expr.multiply`)
- Use `project` to rename or compute fields and limit document size
- Use `limit` early in the pipeline where appropriate
See also: Developer Guide (Aggregation)
