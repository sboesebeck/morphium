package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class UpdateBulkRequest extends BulkRequest {
    private Map<String, Object> query;
    private boolean upsert = false;

    public boolean isUpsert() {
        return upsert;
    }

    public void setUpsert(boolean upsert) {
        this.upsert = upsert;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }

    public void removeOne() {
        if (upsert) throw new IllegalArgumentException("Upsert && remove does not make sense...");

    }

}
