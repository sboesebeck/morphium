package de.caluga.morphium.driver.bson;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MOR-029: {@code BsonEncoder.requireEncodable} answers, without writing a byte, whether the encoder would take a document. The InMemoryDriver uses it to refuse a write
 * the way a wire driver does, before it changes anything. It accepts exactly what {@code encodeDocument} accepts and fails with the same message.
 */
@Tag("core")
public class BsonEncoderRequireEncodableTest {

    public static class Pojo {
        public String name = "a";
    }

    public enum Color { RED }

    private static void accepted(Object value) {
        Map<String, Object> doc = Doc.of("f", value);
        assertDoesNotThrow(() -> BsonEncoder.requireEncodable(doc), String.valueOf(value));
        assertDoesNotThrow(() -> BsonEncoder.encodeDocument(doc), "the encoder takes it, too: " + value);
    }

    private static void rejected(Object value) {
        Map<String, Object> doc = Doc.of("f", value);
        RuntimeException e = assertThrows(RuntimeException.class, () -> BsonEncoder.requireEncodable(doc));
        assertTrue(e.getMessage().startsWith("Unhandled Data type: "), e.getMessage());
        assertTrue(e.getMessage().contains(Pojo.class.getName()), e.getMessage());
        assertThrows(RuntimeException.class, () -> BsonEncoder.encodeDocument(doc), "the encoder refuses it, too");
    }

    @Test
    public void everyKindOfValueTheEncoderTakesIsAccepted() {
        for (Object v : new Object[] {null, "s", 1, 2L, 3.5, 4.5f, (short) 5, (byte) 6, 'c', true, new Date(), new ObjectId(), new MorphiumId(), UUID.randomUUID(), Pattern.compile("a.*"),
                                      new byte[] {1, 2}, new BigDecimal("1.50"), Color.RED, LocalDate.of(2026, 1, 2), LocalTime.of(1, 2), LocalDateTime.of(2026, 1, 2, 3, 4), Instant.EPOCH}) {
            accepted(v);
        }
    }

    @Test
    public void nestedMapsAndCollectionsAreWalked() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("list", new ArrayList<>(List.of(1, "two", new Date())));
        inner.put("set", new LinkedHashSet<>(List.of("x", "y")));
        accepted(inner);
        accepted(List.of(inner, List.of(1, 2)));
    }

    @Test
    public void aPojoIsRefusedWhereverItSits() {
        rejected(new Pojo());
        rejected(List.of(new Pojo()));
        rejected(new LinkedHashSet<>(List.of(new Pojo())));
        rejected(new Pojo[] {new Pojo()});
        rejected(Map.of("x", new Pojo()));
        rejected(Map.of("outer", Map.of("x", new Pojo())));
        rejected(List.of(List.of(Map.of("deep", new Pojo()))));
    }

    @Test
    public void theAnswerForAClassDoesNotDependOnTheOrderOfTheQuestions() {
        // the answer is cached per class: a refused class stays refused, an accepted one stays accepted
        for (int i = 0; i < 3; i++) {
            rejected(new Pojo());
            accepted("s");
            accepted(LocalDate.of(2026, 1, 2));
            rejected(List.of(new Pojo()));
        }
    }

    @Test
    public void theMessageNamesTheClassLikeTheEncoderDoes() {
        RuntimeException viaCheck = assertThrows(RuntimeException.class, () -> BsonEncoder.requireEncodable(Doc.of("f", new Pojo())));
        RuntimeException viaEncoder = assertThrows(RuntimeException.class, () -> BsonEncoder.encodeDocument(Doc.of("f", new Pojo())));
        assertEquals(viaEncoder.getMessage(), viaCheck.getMessage());
    }

    @Test
    public void aNullDocumentIsNotAnError() {
        assertDoesNotThrow(() -> BsonEncoder.requireEncodable(null));
    }
}
