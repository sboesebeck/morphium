package de.caluga.morphium.bulk;

import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;

/**
 * User: Stephan Bösebeck
 * Date: 28.04.14
 * Time: 22:33
 * <p/>
 * TODO: Add documentation here
 */
public class BulkOperationContext {
    private Morphium morphium;

    private boolean ordered;

    private BulkWriteOperation bulk = null;

    public BulkOperationContext(Morphium m, boolean ordered) {
        morphium = m;
        this.ordered = ordered;
    }


    public <T> BulkRequestWrapper addFind(Query<T> q) {
        if (bulk == null) {
            if (ordered) {
                bulk = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).initializeOrderedBulkOperation();
            } else {
                bulk = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).initializeUnorderedBulkOperation();
            }
        }
        return new BulkRequestWrapper(bulk.find(q.toQueryObject()), morphium);
    }

    public BulkWriteResult execute() {
        return bulk.execute();
    }


}
