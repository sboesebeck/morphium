package de.caluga.test.morphium;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Driver;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;

/**
 * {@code reread()} exists to refresh an object with the state that is in the database - reading that
 * from a secondary defeats the purpose: a secondary that has not caught up yet answers with an older
 * version of the document, or with nothing at all, in which case reread() returns null although the
 * document is there. So a reread reads from the primary.
 */
@Tag("driver")
public class RereadReadPreferenceTest {

    @Test
    public void rereadReadsFromThePrimary() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.driverSettings().setDriverName(ReadPreferenceRecordingDriver.driverName);
        cfg.driverSettings().setDefaultReadPreference(ReadPreference.nearest());
        cfg.connectionSettings().setDatabase("reread_read_preference");
        cfg.clusterSettings().setHostSeed(new ArrayList<>());

        try (Morphium morphium = new Morphium(cfg)) {
            ReadPreferenceRecordingDriver driver = (ReadPreferenceRecordingDriver) morphium.getDriver();
            Doc doc = new Doc();
            doc.value = "before";
            morphium.store(doc);
            driver.requestedReadPreferences.clear();

            morphium.reread(doc);

            assertThat(driver.requestedReadPreferences).isNotEmpty();
            assertThat(driver.requestedReadPreferences)
                .allMatch(rp -> rp != null && rp.getType() == ReadPreferenceType.PRIMARY);
        }
    }

    @Entity(collectionName = "reread_docs")
    public static class Doc {
        @Id
        private String id = "the-one-and-only";
        private String value;

        public String getId() {
            return id;
        }

        public String getValue() {
            return value;
        }
    }

    @Driver(name = ReadPreferenceRecordingDriver.driverName, description = "records the read preferences it is asked for")
    public static class ReadPreferenceRecordingDriver extends InMemoryDriver {
        public static final String driverName = "ReadPreferenceRecordingDriver";
        public final List<ReadPreference> requestedReadPreferences = new CopyOnWriteArrayList<>();

        @Override
        public MongoConnection getReadConnection(ReadPreference rp) {
            requestedReadPreferences.add(rp);
            return super.getReadConnection(rp);
        }
    }
}
