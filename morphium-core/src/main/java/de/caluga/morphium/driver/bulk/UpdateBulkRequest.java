package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.driver.Doc;

import java.util.Map;

/**
 * bulk request for updating
 **/
@SuppressWarnings("CommentedOutCode")
public class UpdateBulkRequest extends BulkRequest {
    private Doc query;
    private Doc cmd;

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

    public Doc getQuery() {
        return query;
    }

    public void setQuery(Doc query) {
        this.query = query;
    }

    //    public void setOperation(UpdateOperation up) {
    //        this.op = up;
    //    }
    //
    //    public UpdateOperation getOperation() {
    //        return op;
    //    }

    public Doc getCmd() {
        return cmd;
    }

    public void setCmd(Doc cmd) {
        this.cmd = cmd;
    }
}
