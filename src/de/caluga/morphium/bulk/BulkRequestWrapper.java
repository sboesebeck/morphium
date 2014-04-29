package de.caluga.morphium.bulk;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteRequestBuilder;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.query.Query;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 28.04.14
 * Time: 23:04
 * <p/>
 * TODO: Add documentation here
 */
public class BulkRequestWrapper {

    private BulkWriteRequestBuilder builder;
    private Morphium morphium;
    private BulkOperationContext bc;
    private Query query;

    private MorphiumStorageListener.UpdateTypes updateType;

    public BulkRequestWrapper(BulkWriteRequestBuilder b, Morphium m, BulkOperationContext bulk, Query q) {
        builder = b;
        morphium = m;
        bc = bulk;
        query = q;
    }


    public BulkRequestWrapper upsert() {
        builder.upsert();
        return this;
    }

    public void removeOne() {
        updateType = null;

        builder.removeOne();
    }

    public void replaceOne(Object obj) {
        updateType = MorphiumStorageListener.UpdateTypes.SET;
        builder.replaceOne(morphium.getMapper().marshall(obj));
    }

    public void remove() {
        updateType = null;
        builder.remove();
    }

    private void writeOp(String operation, String field, Object value) {
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class) || morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
            builder.update(new BasicDBObject(operation, new BasicDBObject(field, morphium.getMapper().marshall(value))));
        } else if (Map.class.isAssignableFrom(value.getClass())) {
            BasicDBObject map = new BasicDBObject();
            Map valueMap = (Map) value;
            for (Object o : valueMap.keySet()) {
                map.put((String) o, morphium.getMapper().marshall(valueMap.get(o)));
            }
            builder.update(new BasicDBObject(operation, new BasicDBObject(field, map)));

        } else if (List.class.isAssignableFrom(value.getClass())) {
            //list handling
            BasicDBList lst = new BasicDBList();
            List valList = (List) value;
            for (Object o : valList) {
                lst.add(morphium.getMapper().marshall(o));
            }
            builder.update(new BasicDBObject(operation, new BasicDBObject(field, lst)));
        } else {
            builder.update(new BasicDBObject(operation, new BasicDBObject(field, value)));
        }
    }

    private void writeOpOne(String operation, String field, Object value) {
        if (morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Entity.class) || morphium.getARHelper().isAnnotationPresentInHierarchy(value.getClass(), Embedded.class)) {
            builder.updateOne(new BasicDBObject(operation, new BasicDBObject(field, morphium.getMapper().marshall(value))));
        } else if (Map.class.isAssignableFrom(value.getClass())) {
            BasicDBObject map = new BasicDBObject();
            Map valueMap = (Map) value;
            for (Object o : valueMap.keySet()) {
                map.put((String) o, morphium.getMapper().marshall(valueMap.get(o)));
            }
            builder.updateOne(new BasicDBObject(operation, new BasicDBObject(field, map)));

        } else if (List.class.isAssignableFrom(value.getClass())) {
            //list handling
            BasicDBList lst = new BasicDBList();
            List valList = (List) value;
            for (Object o : valList) {
                lst.add(morphium.getMapper().marshall(o));
            }
            builder.updateOne(new BasicDBObject(operation, new BasicDBObject(field, lst)));
        } else {
            builder.updateOne(new BasicDBObject(operation, new BasicDBObject(field, value)));
        }
    }

    public void set(String field, Object val, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.SET;
        if (multiple) {
            writeOp("$set", field, val);
        } else {
            writeOpOne("$set", field, val);
        }
    }

    public void unset(String field, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.UNSET;

        if (multiple) {
            writeOp("$unset", field, 1);
        } else {
            writeOpOne("$unset", field, 1);
        }
    }

    public void inc(String field, boolean multiple) {
        inc(field, 1, multiple);
    }

    public void inc(String field, int amount, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.INC;
        if (multiple) {
            writeOp("$inc", field, amount);
        } else {
            writeOpOne("$inc", field, amount);
        }
    }

    public void mul(String field, int val, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.MUL;

        if (multiple)
            writeOp("$mul", field, val);
        else
            writeOpOne("$mul", field, val);
    }

    public void min(String field, int val, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.MIN;

        if (multiple)
            writeOp("$min", field, val);
        else
            writeOpOne("$min", field, val);
    }

    public void max(String field, int val, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.MAX;

        if (multiple)
            writeOp("$max", field, val);
        else
            writeOpOne("$max", field, val);
    }


    public void rename(String fieldOld, String fieldNew, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.RENAME;

        if (multiple)
            writeOp("$rename", fieldOld, fieldNew);
        else
            writeOpOne("$rename", fieldOld, fieldNew);
    }

    public void dec(String field, boolean multiple) {

        dec(field, -1, multiple);
    }

    public void dec(String field, int amount, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.DEC;
        if (multiple)
            writeOp("$inc", field, -amount);
        else
            writeOpOne("$inc", field, -amount);
    }

    public void pull(String field, boolean multiple, Object... value) {
        updateType = MorphiumStorageListener.UpdateTypes.PULL;

        if (value.length == 1) {
            if (multiple) {
                writeOp("$pull", field, value[0]);
            } else {
                writeOpOne("$pull", field, value[0]);
            }

        } else {
            if (multiple) {
                writeOp("$pullAll", field, Arrays.asList(value));
            } else {
                writeOpOne("$pullAll", field, Arrays.asList(value));
            }

        }

    }

    public void push(String field, boolean multiple, Object... value) {
        updateType = MorphiumStorageListener.UpdateTypes.PUSH;

        if (value.length == 1) {
            if (multiple) {
                writeOp("push", field, value[0]);
            } else {
                writeOpOne("$push", field, value[0]);
            }

        } else {
            if (multiple) {
                writeOp("$pushAll", field, Arrays.asList(value));
            } else {
                writeOpOne("$pushAll", field, Arrays.asList(value));
            }

        }

    }

    public void pop(String field, boolean first, boolean multiple) {
        updateType = MorphiumStorageListener.UpdateTypes.POP;

        if (multiple)
            writeOp("$pop", field, first ? -1 : 1);
        else
            writeOpOne("$pop", field, first ? -1 : 1);
    }

    public void preExec() {
        if (updateType == null) {
            morphium.firePreRemoveEvent(query);

        } else {
            morphium.firePreUpdateEvent(query.getType(), updateType);
        }

    }

    public MorphiumStorageListener.UpdateTypes getUpdateType() {
        return updateType;
    }

    public Query getQuery() {
        return query;
    }

    public void postExec() {
        if (updateType == null) {
            morphium.firePostRemoveEvent(query);
        } else {
            morphium.firePostUpdateEvent(query.getType(), updateType);
        }
        morphium.getCache().clearCacheIfNecessary(query.getType());
    }
}
