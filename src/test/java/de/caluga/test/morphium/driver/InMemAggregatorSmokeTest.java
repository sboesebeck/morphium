package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.MorphiumId;
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
public class InMemAggregatorSmokeTest {

    private Morphium morphium;
    private InMemoryDriver drv;
    private final String db = "agg_smoke";
    private final String coll = "agg_items";

    @BeforeEach
    public void setup() throws Exception {
        MorphiumConfig cfg = new MorphiumConfig(db, 10, 10000, 1000);
        cfg.setDriverName(InMemoryDriver.driverName);
        morphium = new Morphium(cfg);
        drv = (InMemoryDriver) morphium.getDriver();
        new ClearCollectionCommand(drv).setDb(db).setColl(coll).doClear();
        // seed test data
        new InsertMongoCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setDocuments(Arrays.asList(
                Doc.of("_id", new MorphiumId(), "grp", "A", "value", 1),
                Doc.of("_id", new MorphiumId(), "grp", "A", "value", 3),
                Doc.of("_id", new MorphiumId(), "grp", "B", "value", 5)
            ))
            .execute();
    }

    @AfterEach
    public void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }

    @Entity(collectionName = "agg_items")
    public static class AggItem {
        @Id
        public MorphiumId id;
        public String grp;
        public int value;
    }

    @Test
    public void matchStageWithMap() {
        Aggregator<AggItem, Map> agg = (Aggregator<AggItem, Map>) drv.createAggregator(morphium, AggItem.class, Map.class);
        agg.setCollectionName(coll);
        // $match: { grp: "A" }
        agg.addOperator(UtilsMap.of("$match", Doc.of("grp", "A")));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(2);
        assertThat(res.stream().map(m -> (String) m.get("grp")).collect(Collectors.toSet()))
            .containsExactly("A");
    }

    @Test
    public void groupStageSum() {
        Aggregator<AggItem, Map> agg = (Aggregator<AggItem, Map>) drv.createAggregator(morphium, AggItem.class, Map.class);
        agg.setCollectionName(coll);
        // pipeline: [ { $group: { _id: "$grp", total: { $sum: "$value" } } } ]
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", "$grp",
            "total", Doc.of("$sum", "$value")
        )));

        List<Map<String, Object>> res = agg.aggregateMap();
        // Expect 2 groups: A -> 4, B -> 5
        Map<Object, Integer> totals = res.stream()
            .collect(Collectors.toMap(m -> m.get("_id"), m -> ((Number) m.get("total")).intValue()));

        assertThat(totals.keySet()).containsExactlyInAnyOrder("A", "B");
        assertThat(totals.get("A")).isEqualTo(4);
        assertThat(totals.get("B")).isEqualTo(5);
    }

    @Test
    public void projectStageWithSubDocumentIdField() {
        Aggregator<AggItem, Map> agg = (Aggregator<AggItem, Map>) drv.createAggregator(morphium, AggItem.class, Map.class);
        agg.setCollectionName(coll);
        // group by grp to create an _id sub-document, then project nested _id.someValue into MyValue
        agg.addOperator(UtilsMap.of("$group", Doc.of(
            "_id", Doc.of("someValue", "$grp"),
            "total", Doc.of("$sum", "$value")
        )));
        agg.addOperator(UtilsMap.of("$project", Doc.of(
            "MyValue", "$_id.someValue",
            "total", 1,
            "_id", 0
        )));

        List<Map<String, Object>> res = agg.aggregateMap();
        assertThat(res).hasSize(2);
        assertThat(res).allSatisfy(r -> assertThat(r).doesNotContainKey("_id"));
        Map<Object, Integer> totals = res.stream()
            .collect(Collectors.toMap(m -> m.get("MyValue"), m -> ((Number) m.get("total")).intValue()));
        assertThat(totals.keySet()).containsExactlyInAnyOrder("A", "B");
        assertThat(totals.get("A")).isEqualTo(4);
        assertThat(totals.get("B")).isEqualTo(5);
    }
}
