package de.caluga.test.objectmapping;

import de.caluga.morphium.objectmapping.BigDecimalMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * MongoDB kann numerische Felder als int32 (Integer) oder int64 (Long) speichern —
 * z.B. wenn ein Update/Migration ein BigDecimal-Feld als Ganzzahl-Literal schreibt
 * ({@code price_gross: 0} statt {@code 0.0}). Die alte {@code unmarshall}-Implementierung
 * machte {@code new BigDecimal((double) d)} und warf dann
 * {@code ClassCastException: Integer cannot be cast to Double}. unmarshall muss jeden
 * {@link Number}-Typ (und ein bereits dekodiertes BigDecimal) tolerieren.
 */
public class BigDecimalMapperTest {

    private final BigDecimalMapper mapper = new BigDecimalMapper();

    @Test
    public void unmarshallToleratesIntegerAndLong() {
        assertEquals(0, new BigDecimal("12").compareTo(mapper.unmarshall(Integer.valueOf(12))));
        assertEquals(0, new BigDecimal("12").compareTo(mapper.unmarshall(Long.valueOf(12L))));
    }

    @Test
    public void unmarshallDouble() {
        assertEquals(0, BigDecimal.valueOf(2.5).compareTo(mapper.unmarshall(Double.valueOf(2.5))));
    }

    @Test
    public void unmarshallBigDecimalPassthrough() {
        BigDecimal bd = new BigDecimal("3.14");
        assertEquals(0, bd.compareTo(mapper.unmarshall(bd)));
    }

    @Test
    public void unmarshallNull() {
        assertNull(mapper.unmarshall(null));
    }
}
