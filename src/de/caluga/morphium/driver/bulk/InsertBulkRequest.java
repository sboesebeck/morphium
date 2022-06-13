package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.driver.Doc;

import java.util.List;
import java.util.Map;

/**
 * insert bulk request entry
 **/
public class InsertBulkRequest extends BulkRequest {
    private final List<Doc> toInsert;

    public InsertBulkRequest(List<Doc> objToInsert) {
        toInsert = objToInsert;
    }

    public List<Doc> getToInsert() {
        return toInsert;
    }

}
