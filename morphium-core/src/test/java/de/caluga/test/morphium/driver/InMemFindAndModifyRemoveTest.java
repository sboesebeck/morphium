package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #398: a remove {@code findAndModify} that matches nothing must answer
 * {@code {value: null}} like real MongoDB, not throw {@code IndexOutOfBoundsException} from the
 * unguarded {@code list.get(0)}.
 */
@Tag("inmemory")
public class InMemFindAndModifyRemoveTest {

    private static final String DB = "fam_remove_db";
    private static final String COLL = "locks";

    private InMemoryDriver driver;

    @BeforeEach
    public void setUp() throws Exception {
        driver = new InMemoryDriver();
        driver.connect();
        new ClearCollectionCommand(driver).setDb(DB).setColl(COLL).doClear();
    }

    @AfterEach
    public void tearDown() {
        driver.close();
    }

    @Test
    public void removeMatchingDocumentReturnsItAndDeletesIt() throws Exception {
        insert(Doc.of("_id", "lock-1", "owner", "alice"));

        Map<String, Object> removed = findAndModifyRemove(Doc.of("_id", "lock-1"));

        assertThat(removed).as("remove findAndModify must return the removed document").isNotNull();
        assertThat(removed.get("_id")).isEqualTo("lock-1");
        assertThat(removed.get("owner")).isEqualTo("alice");
        assertThat(findAll()).as("the matched document must be gone").isEmpty();
    }

    @Test
    public void removeWithNoMatchReturnsNullAndKeepsCollection() throws Exception {
        insert(Doc.of("_id", "lock-2", "owner", "bob"));

        Map<String, Object> removed = findAndModifyRemove(Doc.of("_id", "does-not-exist"));

        assertThat(removed).as("no match must yield value:null, not a crash").isNull();
        assertThat(findAll()).as("an unmatched remove must not touch the collection").hasSize(1);
        assertThat(findAll().get(0).get("_id")).isEqualTo("lock-2");
    }

    @Test
    public void removeOnEmptyCollectionReturnsNull() throws Exception {
        Map<String, Object> removed = findAndModifyRemove(Doc.of("_id", "anything"));

        assertThat(removed).isNull();
        assertThat(findAll()).isEmpty();
    }

    // --- helpers -------------------------------------------------------------

    private Map<String, Object> findAndModifyRemove(Map<String, Object> filter) throws Exception {
        return new FindAndModifyMongoCommand(driver)
                .setDb(DB).setColl(COLL)
                .setQuery(filter)
                .setRemove(true)
                .execute();
    }

    private void insert(Map<String, Object> doc) throws Exception {
        new InsertMongoCommand(driver).setDb(DB).setColl(COLL)
                .setDocuments(List.of(doc))
                .execute();
    }

    private List<Map<String, Object>> findAll() throws Exception {
        return new FindCommand(driver).setDb(DB).setColl(COLL).setFilter(Doc.of()).execute();
    }
}
