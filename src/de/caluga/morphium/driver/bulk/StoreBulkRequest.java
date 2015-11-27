package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 27.11.15.
 */

import java.util.List;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class StoreBulkRequest extends InsertBulkRequest {

    public StoreBulkRequest(List<Map<String, Object>> objToStore) {
        super(objToStore);
    }
}
