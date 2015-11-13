package de.caluga.morphium.driver.mongodb;

import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.BulkWriteUpsert;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.WriteAccessType;
import de.caluga.morphium.query.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 28.04.14
 * Time: 22:33
 * <p/>
 * TODO: Add documentation here
 */
public class BulkOperationContextMongo {
    private Morphium morphium;

    private boolean ordered;

    private BulkWriteOperation bulk = null;
    private List<BulkRequestWrapper> requests;

    public BulkOperationContextMongo(Morphium m, boolean ordered) {
        morphium = m;
        this.ordered = ordered;
        requests = new ArrayList<>();
    }

    public <T> void insert(T o) {
        if (bulk == null) {
            if (ordered) {
                bulk = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(o.getClass())).initializeOrderedBulkOperation();
            } else {
                bulk = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(o.getClass())).initializeUnorderedBulkOperation();
            }
        }
        bulk.insert(morphium.getMapper().marshall(o));
    }

    public <T> BulkRequestWrapper addFind(Query<T> q) {
        if (bulk == null) {
            if (ordered) {
                bulk = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).initializeOrderedBulkOperation();
            } else {
                bulk = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(q.getType())).initializeUnorderedBulkOperation();
            }
        }
        BulkRequestWrapper w = new BulkRequestWrapper(bulk.find(q.toQueryObject()), morphium, q);
        requests.add(w);
        return w;
    }

    public BulkWriteResult execute() {
        if (bulk == null) return new BulkWriteResult() {
            @Override
            public boolean isAcknowledged() {
                return false;
            }

            @Override
            public int getInsertedCount() {
                return 0;
            }

            @Override
            public int getMatchedCount() {
                return 0;
            }

            @Override
            public int getRemovedCount() {
                return 0;
            }

            @Override
            public boolean isModifiedCountAvailable() {
                return false;
            }

            @Override
            public int getModifiedCount() {
                return 0;
            }

            @Override
            public List<BulkWriteUpsert> getUpserts() {
                return null;
            }
        };
        for (BulkRequestWrapper w : requests) {
            w.preExec();
        }
        long dur = System.currentTimeMillis();
        BulkWriteResult res = bulk.execute();
        dur = System.currentTimeMillis() - dur;
        for (BulkRequestWrapper w : requests) {
            w.postExec();
        }
        for (BulkRequestWrapper w : requests) {
            morphium.fireProfilingWriteEvent(w.getQuery().getType(), this, dur, false, WriteAccessType.BULK_UPDATE);
        }
        return res;
    }

}
