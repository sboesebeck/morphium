package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import java.util.Map;

/**
 * bulk request for updating
 **/
public class UpdateBulkRequest extends BulkRequest {
    private Map<String, Object> query;
    private Map<String, Object> cmd;

    //    public enum UpdateOperation {
    //        set, inc, pop, push, unset, rename, max, min, mul,
    //    }

    //    private UpdateOperation op;
    private boolean upsert = false;
    private boolean multiple = false;

    public boolean isMultiple() {
        return multiple;
    }

    public void setMultiple(boolean multiple) {
        this.multiple = multiple;
    }

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

    //    public void setOperation(UpdateOperation up) {
    //        this.op = up;
    //    }
    //
    //    public UpdateOperation getOperation() {
    //        return op;
    //    }

    public Map<String, Object> getCmd() {
        return cmd;
    }

    public void setCmd(Map<String, Object> cmd) {
        this.cmd = cmd;
    }
}
