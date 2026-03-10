package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("inmemory")
public class InMemGeoIntersectsTest {
    private final String db = "geodb";
    private final String coll = "geocoll";

    @Test
    public void pointInsidePolygonMatches() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();

        Map<String, Object> polygon = Doc.of(
            "type", "Polygon",
            "coordinates", List.of(
                List.of(
                    List.of(0.0, 0.0),
                    List.of(10.0, 0.0),
                    List.of(10.0, 10.0),
                    List.of(0.0, 10.0),
                    List.of(0.0, 0.0)
                )
            )
        );

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("shape", polygon)))
            .execute();

        Map<String, Object> query = Doc.of(
            "$geometry", Doc.of(
                "type", "Point",
                "coordinates", List.of(5.0, 5.0)
            )
        );

        List<Map<String, Object>> result = new FindCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setFilter(Doc.of("shape", Doc.of("$geoIntersects", query)))
            .execute();

        assertEquals(1, result.size());
    }

    @Test
    public void pointOutsidePolygonDoesNotMatch() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();

        Map<String, Object> polygon = Doc.of(
            "type", "Polygon",
            "coordinates", List.of(
                List.of(
                    List.of(0.0, 0.0),
                    List.of(10.0, 0.0),
                    List.of(10.0, 10.0),
                    List.of(0.0, 10.0),
                    List.of(0.0, 0.0)
                )
            )
        );

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("shape", polygon)))
            .execute();

        Map<String, Object> query = Doc.of(
            "$geometry", Doc.of(
                "type", "Point",
                "coordinates", List.of(20.0, 20.0)
            )
        );

        List<Map<String, Object>> result = new FindCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setFilter(Doc.of("shape", Doc.of("$geoIntersects", query)))
            .execute();

        assertEquals(0, result.size());
    }

    @Test
    public void polygonIntersectingLineMatches() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();

        Map<String, Object> line = Doc.of(
            "type", "LineString",
            "coordinates", List.of(
                List.of(-5.0, 5.0),
                List.of(15.0, 5.0)
            )
        );

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("shape", line)))
            .execute();

        Map<String, Object> polygon = Doc.of(
            "$geometry", Doc.of(
                "type", "Polygon",
                "coordinates", List.of(
                    List.of(
                        List.of(0.0, 0.0),
                        List.of(10.0, 0.0),
                        List.of(10.0, 10.0),
                        List.of(0.0, 10.0),
                        List.of(0.0, 0.0)
                    )
                )
            )
        );

        List<Map<String, Object>> result = new FindCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setFilter(Doc.of("shape", Doc.of("$geoIntersects", polygon)))
            .execute();

        assertEquals(1, result.size());
    }

    @Test
    public void multiPointIntersectsPolygon() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();

        Map<String, Object> polygon = Doc.of(
            "type", "Polygon",
            "coordinates", List.of(
                List.of(
                    List.of(0.0, 0.0),
                    List.of(10.0, 0.0),
                    List.of(10.0, 10.0),
                    List.of(0.0, 10.0),
                    List.of(0.0, 0.0)
                )
            )
        );

        new InsertMongoCommand(drv).setDb(db).setColl(coll)
            .setDocuments(List.of(Doc.of("shape", polygon)))
            .execute();

        Map<String, Object> multiPoint = Doc.of(
            "$geometry", Doc.of(
                "type", "MultiPoint",
                "coordinates", List.of(
                    List.of(20.0, 20.0),
                    List.of(5.0, 5.0)
                )
            )
        );

        List<Map<String, Object>> result = new FindCommand(drv)
            .setDb(db)
            .setColl(coll)
            .setFilter(Doc.of("shape", Doc.of("$geoIntersects", multiPoint)))
            .execute();

        assertEquals(1, result.size());
    }
}
