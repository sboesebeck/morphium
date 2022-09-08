package de.caluga.morphium.driver.bulk;

import de.caluga.morphium.driver.Doc;

import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 13.11.15
 * Time: 23:35
 * <p>
 * delete bulk request entry
 */
public class DeleteBulkRequest extends BulkRequest {
    private Doc query;
    private boolean multiple = false;

    public boolean isMultiple() {
        return multiple;
    }

    public void setMultiple(boolean multiple) {
        this.multiple = multiple;
    }

    public Doc getQuery() {
        return query;
    }

    public void setQuery(Doc query) {
        this.query = query;
    }
}
