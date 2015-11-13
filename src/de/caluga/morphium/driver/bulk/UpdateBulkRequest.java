package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.driver.MorphiumDriver;

import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public abstract class UpdateBulkRequest extends BulkRequest {
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

    public abstract Map<String, Object> execute(MorphiumDriver drv);

}
