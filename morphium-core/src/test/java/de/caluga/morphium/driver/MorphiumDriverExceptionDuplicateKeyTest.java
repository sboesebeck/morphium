package de.caluga.morphium.driver;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class MorphiumDriverExceptionDuplicateKeyTest {

    @Test
    public void theMongoCodeOfTheExceptionCountsInAnyNumberType() {
        for (Object code : new Object[] {11000, 11000L, 11000.0, (short) 11000}) {
            MorphiumDriverException e = new MorphiumDriverException("E11000 duplicate key");
            e.setMongoCode(code);
            assertTrue(e.isDuplicateKey(), String.valueOf(code));
        }
    }

    @Test
    public void aWriteErrorWithCode11000Counts() {
        MorphiumDriverException e = new MorphiumDriverException("write failed");
        e.setWriteErrors(List.of(Map.of("code", 12345), Map.of("code", 11000L)));
        assertTrue(e.isDuplicateKey());
    }

    @Test
    public void otherCodesAndMissingInformationDoNot() {
        assertFalse(new MorphiumDriverException("x").isDuplicateKey());
        MorphiumDriverException e = new MorphiumDriverException("x");
        e.setMongoCode(66);
        e.setWriteErrors(List.of(Map.of("code", 52)));
        assertFalse(e.isDuplicateKey());
        e.setMongoCode("11000");
        assertFalse(e.isDuplicateKey(), "a String code is not a number");
        e.setMongoCode(null);
        e.setWriteErrors(java.util.Collections.singletonList(null));
        assertFalse(e.isDuplicateKey());
    }

    @Test
    public void theCauseChainIsNotSearched() {
        MorphiumDriverException inner = new MorphiumDriverException("inner");
        inner.setMongoCode(11000);
        assertFalse(new MorphiumDriverException("outer", inner).isDuplicateKey());
    }
}
