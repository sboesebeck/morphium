package de.caluga.poppydb.netty;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** buildInfo.versionArray: mongorestore refuses servers reporting fewer than 3 entries. */
@Tag("poppydb")
public class BuildVersionArrayTest {

    @Test
    public void snapshotVersionParses() {
        assertEquals(List.of(6, 3, 2, 0), MongoCommandHandler.buildVersionArray("6.3.2-SNAPSHOT"));
    }

    @Test
    public void releaseVersionParses() {
        assertEquals(List.of(6, 3, 1, 0), MongoCommandHandler.buildVersionArray("6.3.1"));
    }

    @Test
    public void devFallbackStillHasFourEntries() {
        assertEquals(List.of(0, 0, 0, 0), MongoCommandHandler.buildVersionArray("0.0.0-dev"));
    }

    @Test
    public void garbageYieldsZeros() {
        assertEquals(List.of(0, 0, 0, 0), MongoCommandHandler.buildVersionArray("weird"));
    }
}
