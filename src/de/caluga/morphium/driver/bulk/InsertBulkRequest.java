package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import java.util.List;
import java.util.Map;

/**
 * insert bulk request entry
 **/
public class InsertBulkRequest extends BulkRequest {
    private List<Map<String, Object>> toInsert;

    public InsertBulkRequest(List<Map<String, Object>> objToInsert) {
        toInsert = objToInsert;
    }

    public List<Map<String, Object>> getToInsert() {
        return toInsert;
    }

    @SuppressWarnings("unused")
    public void setToInsert(List<Map<String, Object>> toInsert) {
        this.toInsert = toInsert;
    }

}
