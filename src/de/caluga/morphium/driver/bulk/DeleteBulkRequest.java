package de.caluga.morphium.driver.bulk;

import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 13.11.15
 * Time: 23:35
 * <p>
 * delete bulk request entry
 */
public class DeleteBulkRequest extends BulkRequest {
    private Map<String, Object> query;
    private boolean multiple;

    public boolean isMultiple() {
        return multiple;
    }

    public void setMultiple(boolean multiple) {
        this.multiple = multiple;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }
}
