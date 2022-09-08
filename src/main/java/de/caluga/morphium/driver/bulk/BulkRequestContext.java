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


    @SuppressWarnings("RedundantThrows")
    public abstract Map<String, Object> execute() throws MorphiumDriverException;

    public abstract UpdateBulkRequest addUpdateBulkRequest();

    public abstract InsertBulkRequest addInsertBulkRequest(List<Map<String, Object>> toInsert);

    public abstract DeleteBulkRequest addDeleteBulkRequest();

}
