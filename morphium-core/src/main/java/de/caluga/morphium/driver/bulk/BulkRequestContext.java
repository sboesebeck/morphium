package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.BulkWriteException;
import de.caluga.morphium.driver.MorphiumDriverException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * context for doing bulk request.
 **/
@SuppressWarnings("WeakerAccess")
public abstract class BulkRequestContext {
    private final Morphium morphium;

    public BulkRequestContext(Morphium m) {
        morphium = m;
    }

    public Morphium getMorphium() {
        return morphium;
    }

    /**
     * MOR-059: the end of an {@code execute()}. The failures of the requests were collected (an ordered context stops at the first one, an unordered one runs every
     * request); with {@code WriterSettings.raiseOnWriteErrors} (default) one {@link BulkWriteException} carrying the counts of what was applied is thrown, with the switch off
     * they are only logged, as before.
     */
    @SuppressWarnings("deprecation")
    protected final void raiseOrLog(String db, String collection, boolean ordered, int requestCount, Map<String, Object> partialResult, List<MorphiumDriverException> failures) {
        if (failures.isEmpty()) {
            return;
        }

        Logger log = LoggerFactory.getLogger(BulkRequestContext.class);
        failures.forEach(f -> log.error("A request of the bulk write on {} was rejected", collection, f));

        if (morphium == null || morphium.getConfig() == null || !morphium.getConfig().writerSettings().isRaiseOnWriteErrors()) {
            return;
        }

        throw new BulkWriteException("Bulk write on " + collection + " failed: " + failures.size() + " of " + requestCount + " requests rejected, first: " + failures.get(0).getMessage()
                                     + "; applied: " + partialResult + "; " + (ordered ? "ordered: the remaining requests were not executed" : "unordered: all other requests were executed")
                                     + ", requests that were applied are not rolled back", partialResult, failures);
    }


    @SuppressWarnings("RedundantThrows")
    public abstract Map<String, Object> execute() throws MorphiumDriverException;

    public abstract UpdateBulkRequest addUpdateBulkRequest();

    public abstract InsertBulkRequest addInsertBulkRequest(List<Map<String, Object>> toInsert);

    public abstract DeleteBulkRequest addDeleteBulkRequest();

}
