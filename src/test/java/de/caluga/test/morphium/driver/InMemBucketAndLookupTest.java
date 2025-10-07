package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("inmemory")
public class InMemBucketAndLookupTest {

    private Morphium morphium;
    private InMemoryDriver drv;
    private final String db = "agg_bucket_lookup";
    private final String valuesColl = "bucket_values";
    private final String ordersColl = "orders";
    private final String inventoryColl = "inventory";

    @BeforeEach
    public void setup() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig(db, 10, 10000, 1000);
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        drv = (InMemoryDriver) morphium.getDriver();

        new ClearCollectionCommand(drv).setDb(db).setColl(valuesColl).doClear();
        new ClearCollectionCommand(drv).setDb(db).setColl(ordersColl).doClear();
        new ClearCollectionCommand(drv).setDb(db).setColl(inventoryColl).doClear();
    }

    @AfterEach
    public void cleanup() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Entity(collectionName = "bucket_values")
    public static class BucketValueDoc {
        @Id
        public MorphiumId id;
        public int score;
    }

    @Entity(collectionName = "orders")
    public static class OrderDoc {
        @Id
        public MorphiumId id;
        public String sku;
        public int quantity;
        public String itemId;
    }

    @Test
    public void bucketStageDistributesDocumentsIntoBoundaries() throws Exception {
        new InsertMongoCommand(drv)
            .setDb(db)
            .setColl(valuesColl)
            .setDocuments(Arrays.asList(
                Doc.of("_id", new MorphiumId(), "score", 1),
                Doc.of("_id", new MorphiumId(), "score", 3),
                Doc.of("_id", new MorphiumId(), "score", 5),
                Doc.of("_id", new MorphiumId(), "score", 8)
            ))
            .execute();

        Aggregator<BucketValueDoc, Map> agg = (Aggregator<BucketValueDoc, Map>) drv.createAggregator(morphium, BucketValueDoc.class, Map.class);
        agg.setCollectionName(valuesColl);
        agg.addOperator(UtilsMap.of("$bucket", Doc.of(
            "groupBy", "$score",
            "boundaries", List.of(0, 4, 6),
            "default", "other",
            "output", Doc.of("count", Doc.of("$sum", 1))
        )));

        List<Map<String, Object>> results = agg.aggregateMap();

        Map<Object, Integer> bucketCounts = results.stream()
            .collect(Collectors.toMap(m -> m.get("_id"), m -> ((Number) m.get("count")).intValue()));

        assertThat(bucketCounts).containsEntry(0, 2);   // scores 1 and 3 fall into [0,4)
        assertThat(bucketCounts).containsEntry(4, 1);   // score 5 falls into [4,6)
        assertThat(bucketCounts).containsEntry("other", 1); // score 8 goes to default
    }

    @Test
    public void simpleLookupJoinsForeignDocuments() throws Exception {
        new InsertMongoCommand(drv)
            .setDb(db)
            .setColl(ordersColl)
            .setDocuments(Arrays.asList(
                Doc.of("_id", new MorphiumId(), "item_id", "A1", "sku", "A", "quantity", 2),
                Doc.of("_id", new MorphiumId(), "item_id", "B2", "sku", "B", "quantity", 1)
            ))
            .execute();

        Map<String, List<Map<String, Object>>> dbSnapshot = drv.getDatabase(db);
        assertThat(dbSnapshot).isNotNull();
        List<Map<String, Object>> storedOrders = dbSnapshot.get(ordersColl);
        assertThat(storedOrders).isNotNull();
        assertThat(storedOrders).isNotEmpty();
        assertThat(storedOrders.get(0)).containsKey("item_id");

        List<Map<String, Object>> mappedOrders = morphium.createQueryFor(OrderDoc.class)
            .setCollectionName(ordersColl)
            .asMapList();
        assertThat(mappedOrders).isNotEmpty();
        String localFieldName = morphium.getARHelper().getMongoFieldName(OrderDoc.class, "itemId");
        assertThat(mappedOrders.get(0)).containsKey(localFieldName);

        new InsertMongoCommand(drv)
            .setDb(db)
            .setColl(inventoryColl)
            .setDocuments(Arrays.asList(
                Doc.of("_id", "A1", "sku", "A", "stock", 5),
                Doc.of("_id", "B2", "sku", "B", "stock", 3),
                Doc.of("_id", "C3", "sku", "C", "stock", 7)
            ))
            .execute();

        Aggregator<OrderDoc, Map> agg = (Aggregator<OrderDoc, Map>) drv.createAggregator(morphium, OrderDoc.class, Map.class);
        agg.setCollectionName(ordersColl);
        agg.addOperator(UtilsMap.of("$lookup", Doc.of(
            "from", inventoryColl,
            "localField", localFieldName,
            "foreignField", "_id",
            "as", "inventory"
        )));

        List<Map<String, Object>> results = agg.aggregateMap();
        assertThat(results).hasSize(2);
        for (Map<String, Object> doc : results) {
            List<?> inventory = (List<?>) doc.get("inventory");
            assertThat(inventory).describedAs("doc=%s", doc).hasSize(1);
            Map<?, ?> joined = (Map<?, ?>) inventory.get(0);
            Object localValue = doc.get(localFieldName);
            if (localValue == null) {
                localValue = doc.get("itemId");
            }
            assertThat(localValue).isNotNull();
            assertThat(joined.get("_id")).isEqualTo(localValue);
        }
    }

    @Test
    public void lookupWithPipelineAndLetFiltersMatches() throws Exception {
        new InsertMongoCommand(drv)
            .setDb(db)
            .setColl(ordersColl)
            .setDocuments(Arrays.asList(
                Doc.of("_id", new MorphiumId(), "sku", "SKU-1", "quantity", 5),
                Doc.of("_id", new MorphiumId(), "sku", "SKU-2", "quantity", 12)
            ))
            .execute();

        new InsertMongoCommand(drv)
            .setDb(db)
            .setColl(inventoryColl)
            .setDocuments(Arrays.asList(
                Doc.of("_id", "WH1", "sku", "SKU-1", "stock", 10, "warehouse", "WH1"),
                Doc.of("_id", "WH2", "sku", "SKU-1", "stock", 3, "warehouse", "WH2"),
                Doc.of("_id", "WH3", "sku", "SKU-2", "stock", 20, "warehouse", "WH3")
            ))
            .execute();

        Map<String, Object> exprQuery = Doc.of("$expr", Doc.of(
            "$and", List.of(
                Doc.of("$eq", List.of("$sku", "SKU-1")),
                Doc.of("$gte", List.of("$stock", 5))
            )
        ));
        assertThat(de.caluga.morphium.driver.inmem.QueryHelper.matchesQuery(exprQuery, Doc.of("sku", "SKU-1", "stock", 10), null)).isTrue();

        Aggregator<OrderDoc, Map> agg = (Aggregator<OrderDoc, Map>) drv.createAggregator(morphium, OrderDoc.class, Map.class);
        agg.setCollectionName(ordersColl);

        String skuField = morphium.getARHelper().getMongoFieldName(OrderDoc.class, "sku");
        String qtyField = morphium.getARHelper().getMongoFieldName(OrderDoc.class, "quantity");

        Map<String, Object> lookupStage = Doc.of(
            "from", inventoryColl,
            "let", Doc.of("order_qty", "$" + qtyField, "order_sku", "$" + skuField),
            "pipeline", List.of(
                Doc.of("$match", Doc.of(
                    "$expr", Doc.of(
                        "$and", List.of(
                            Doc.of("$eq", List.of("$sku", "$$order_sku")),
                            Doc.of("$gte", List.of("$stock", "$$order_qty"))
                        )
                    )
                ))
            ),
            "as", "stock"
        );

        agg.addOperator(UtilsMap.of("$lookup", lookupStage));

        List<Map<String, Object>> results = agg.aggregateMap();
        assertThat(results).hasSize(2);

        Map<String, List<Map<String, Object>>> stockMap = results.stream()
            .collect(Collectors.toMap(
                doc -> (String) doc.get("sku"),
                doc -> (List<Map<String, Object>>) doc.get("stock")
            ));

        assertThat(stockMap.get("SKU-1")).describedAs("results=%s", results).hasSize(1);
        assertThat(stockMap.get("SKU-1").get(0).get("warehouse")).isEqualTo("WH1");
        assertThat(stockMap.get("SKU-2")).describedAs("results=%s", results).hasSize(1);
        assertThat(stockMap.get("SKU-2").get(0).get("warehouse")).isEqualTo("WH3");
    }
}
