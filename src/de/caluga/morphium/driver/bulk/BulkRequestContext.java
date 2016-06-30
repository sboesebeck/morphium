package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.List;
import java.util.Map;

/**
 * context for doing bulk request.
 **/
@SuppressWarnings("WeakerAccess")
public abstract class BulkRequestContext {
    private final Morphium morphium;
    private boolean odererd = false;
    private int batchSize;

    public BulkRequestContext(Morphium m) {
        morphium = m;
    }

    public Morphium getMorphium() {
        return morphium;
    }

    @SuppressWarnings("unused")
    public boolean isOdererd() {
        return odererd;
    }

    @SuppressWarnings("unused")
    public void setOdererd(boolean odererd) {
        this.odererd = odererd;
    }

    @SuppressWarnings("unused")
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public abstract Map<String, Object> execute() throws MorphiumDriverException;

    public abstract UpdateBulkRequest addUpdateBulkRequest();

    @SuppressWarnings("unused")
    public abstract StoreBulkRequest addStoreBulkRequest(List<Map<String, Object>> toStore);

    public abstract InsertBulkRequest addInsertBulkReqpest(List<Map<String, Object>> toInsert);

    public abstract DeleteBulkRequest addDeleteBulkRequest();

}
