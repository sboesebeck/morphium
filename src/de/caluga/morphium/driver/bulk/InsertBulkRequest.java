package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class InsertBulkRequest extends BulkRequest {
    private Map<String, Object> toInsert;

    public InsertBulkRequest(Map<String, Object> objToInsert) {
        toInsert = objToInsert;
    }

    public Map<String, Object> getToInsert() {
        return toInsert;
    }

    public void setToInsert(Map<String, Object> toInsert) {
        this.toInsert = toInsert;
    }
}
