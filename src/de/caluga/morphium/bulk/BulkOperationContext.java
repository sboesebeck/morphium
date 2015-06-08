package de.caluga.morphium.bulk;

import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.BulkWriteUpsert;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.WriteAccessType;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

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
    private List<BulkRequestWrapper> requests;

    public BulkOperationContext(Morphium m, boolean ordered) {
        morphium = m;
        this.ordered = ordered;
        requests = new ArrayList<BulkRequestWrapper>();
    }

    public <T> void insert(T o) {
        if (bulk == null) {
            if (ordered) {
                bulk = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(o.getClass())).initializeOrderedBulkOperation();
            } else {
                bulk = morphium.getDatabase().getCollection(morphium.getMapper().getCollectionName(o.getClass())).initializeUnorderedBulkOperation();
            }
        }
        if (morphium.getARHelper().getId(o) == null) {
            Field idField = morphium.getARHelper().getIdField(o);
            if (idField.getType().equals(ObjectId.class)) {
                //create new ID
                ObjectId id = new ObjectId();
                try {
                    idField.set(o, id);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
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
        BulkRequestWrapper w = new BulkRequestWrapper(bulk.find(q.toQueryObject()), morphium, this, q);
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
