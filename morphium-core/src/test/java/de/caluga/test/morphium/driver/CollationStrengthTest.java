package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.inmem.QueryHelper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.text.Collator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MongoDB collation strength is 1-5 (primary..identical), java.text.Collator strength is
 * 0-3 (PRIMARY..IDENTICAL). Passing the mongo value through unmapped shifts every level by
 * one (mongo 1 became SECONDARY) and made strength 4/5 throw IllegalArgumentException.
 */
@Tag("inmemory")
public class CollationStrengthTest {
    private final String db = "colstrength";
    private final String coll = "docs";

    @Test
    public void mongoStrengthMapsToJavaCollatorStrength() {
        assertEquals(Collator.PRIMARY, QueryHelper.getCollator(Doc.of("locale", "en", "strength", 1)).getStrength());
        assertEquals(Collator.SECONDARY, QueryHelper.getCollator(Doc.of("locale", "en", "strength", 2)).getStrength());
        assertEquals(Collator.TERTIARY, QueryHelper.getCollator(Doc.of("locale", "en", "strength", 3)).getStrength());
        // java.text.Collator has no quaternary level - 4 and 5 both map to IDENTICAL,
        // the closest level that is at least as strong as what mongo promises.
        assertEquals(Collator.IDENTICAL, QueryHelper.getCollator(Doc.of("locale", "en", "strength", 4)).getStrength());
        assertEquals(Collator.IDENTICAL, QueryHelper.getCollator(Doc.of("locale", "en", "strength", 5)).getStrength());
    }

    @Test
    public void strengthOneIgnoresDiacritics() throws Exception {
        var drv = seededDriver(Doc.of("name", "résumé"));
        List<Map<String, Object>> res = find(drv, Doc.of("name", "resume"), Doc.of("locale", "en", "strength", 1));
        assertEquals(1, res.size(), "strength 1 (primary) must ignore diacritics: 'resume' matches 'résumé'");
    }

    @Test
    public void strengthTwoIgnoresCaseButNotDiacritics() throws Exception {
        var drv = seededDriver(Doc.of("name", "hello"), Doc.of("name", "héllo"));
        List<Map<String, Object>> res = find(drv, Doc.of("name", "HELLO"), Doc.of("locale", "en", "strength", 2));
        assertEquals(1, res.size(), "strength 2 (secondary) must ignore case but keep diacritics significant");
    }

    @Test
    public void strengthFiveIsAcceptedAndCaseSensitive() throws Exception {
        var drv = seededDriver(Doc.of("name", "hello"));
        List<Map<String, Object>> res = find(drv, Doc.of("name", "HELLO"), Doc.of("locale", "en", "strength", 5));
        assertEquals(0, res.size(), "strength 5 (identical) must be accepted and stay case sensitive");
    }

    private InMemoryDriver seededDriver(Map<String, Object>... docs) throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        new InsertMongoCommand(drv).setDb(db).setColl(coll).setDocuments(List.of(docs)).execute();
        return drv;
    }

    private List<Map<String, Object>> find(InMemoryDriver drv, Map<String, Object> filter,
                                           Map<String, Object> collation) throws Exception {
        FindCommand fnd = new FindCommand(drv).setDb(db).setColl(coll).setFilter(filter).setCollation(collation);
        List<Map<String, Object>> res = fnd.execute();
        fnd.releaseConnection();
        return res;
    }
}
