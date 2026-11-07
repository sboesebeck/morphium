package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;

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
     * Execute the queued requests. Implementations must throw if any request failed or its
     * outcome cannot be confirmed; a normal return means all requests completed successfully.
     * An exception can still mean that some requests were applied.
     */
    @SuppressWarnings("RedundantThrows")
    public abstract Map<String, Object> execute() throws MorphiumDriverException;

    protected static void requireSuccessfulWrite(Map<String, Object> result) {
        if (result == null || !(result.get("ok") instanceof Number)
                || ((Number) result.get("ok")).doubleValue() != 1.0
                || (result.get("writeErrors") instanceof List<?> errors && !errors.isEmpty())
                || result.get("writeConcernError") != null
                || (result.get("writeConcernErrors") instanceof List<?> errors && !errors.isEmpty())) {
            throw new MorphiumDriverException("Bulk write was not confirmed: " + result);
        }
    }

    public abstract UpdateBulkRequest addUpdateBulkRequest();

    public abstract InsertBulkRequest addInsertBulkRequest(List<Map<String, Object>> toInsert);

    public abstract DeleteBulkRequest addDeleteBulkRequest();

}
