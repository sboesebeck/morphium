package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.BulkWriteException;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.WriteReplyErrors;

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
     * request); one {@link BulkWriteException} carrying the counts of what was applied is thrown. */
    protected final void raiseRejected(String db, String collection, boolean ordered, int requestCount, Map<String, Object> partialResult, List<MorphiumDriverException> failures) {
        if (failures.isEmpty()) {
            return;
        }

        Logger log = LoggerFactory.getLogger(BulkRequestContext.class);
        failures.forEach(f -> log.error("A request of the bulk write on {} was rejected", collection, f));

        throw new BulkWriteException("Bulk write on " + collection + " failed: " + failures.size() + " of " + requestCount + " requests rejected, first: " + failures.get(0).getMessage()
                                     + "; applied: " + partialResult + "; " + (ordered ? "ordered: the remaining requests were not executed" : "unordered: all other requests were executed")
                                     + ", requests that were applied are not rolled back", partialResult, failures);
    }


    /**
     * Execute the queued requests. Implementations must throw if any request failed or its
     * outcome cannot be confirmed; a normal return means all requests completed successfully.
     * An exception can still mean that some requests were applied.
     */
    @SuppressWarnings("RedundantThrows")
    public abstract Map<String, Object> execute() throws MorphiumDriverException;

    /**
     * A reply that reports a rejection raises the exception of {@link WriteReplyErrors} (duplicate key, write concern not satisfied, ...); a reply that does not confirm
     * the write ({@code ok} is not 1, or no reply) raises as well, because the outcome cannot be told.
     */
    protected static void requireConfirmed(String db, String collection, Map<String, Object> result) throws MorphiumDriverException {
        MorphiumDriverException rejected = WriteReplyErrors.rejectionOf(db, collection, result);

        if (rejected != null) {
            throw rejected;
        }

        if (result == null || !(result.get("ok") instanceof Number) || ((Number) result.get("ok")).doubleValue() != 1.0) {
            throw new MorphiumDriverException("Bulk write was not confirmed: " + result);
        }
    }

    public abstract UpdateBulkRequest addUpdateBulkRequest();

    public abstract InsertBulkRequest addInsertBulkRequest(List<Map<String, Object>> toInsert);

    public abstract DeleteBulkRequest addDeleteBulkRequest();

}
