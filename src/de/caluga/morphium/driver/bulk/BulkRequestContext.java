package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.Morphium;

import java.util.List;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public abstract class BulkRequestContext {
    private boolean odererd = false;
    private int batchSize;
    private Morphium morphium;

    public BulkRequestContext(Morphium m) {
        morphium = m;
    }

    public Morphium getMorphium() {
        return morphium;
    }

    public boolean isOdererd() {
        return odererd;
    }

    public void setOdererd(boolean odererd) {
        this.odererd = odererd;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public abstract Map<String, Object> execute();

    public abstract UpdateBulkRequest addUpdateBulkRequest();

    public abstract InsertBulkRequest addInsertBulkReqpest(List<Map<String, Object>> toInsert);

    public abstract DeleteBulkRequest addDeleteBulkRequest();

}
