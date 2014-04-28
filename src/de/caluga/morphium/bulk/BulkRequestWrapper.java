package de.caluga.morphium.bulk;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteRequestBuilder;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;

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

    public BulkRequestWrapper(BulkWriteRequestBuilder b, Morphium m) {
        builder = b;
        morphium = m;
    }


    public BulkRequestWrapper upsert() {
        builder.upsert();
        return this;
    }

    public void removeOne() {
        builder.removeOne();
    }

    public void replaceOne(Object obj) {
        builder.replaceOne(morphium.getMapper().marshall(obj));
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
        if (multiple) {
            writeOp("$set", field, val);
        } else {
            writeOpOne("$set", field, val);
        }
    }

    public void unset(String field, boolean multiple) {
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
        if (multiple) {
            writeOp("$inc", field, amount);
        } else {
            writeOpOne("$inc", field, amount);
        }
    }

    public void mul(String field, int val, boolean multiple) {
        if (multiple)
            writeOp("$mul", field, val);
        else
            writeOpOne("$mul", field, val);
    }

    public void min(String field, int val, boolean multiple) {
        if (multiple)
            writeOp("$min", field, val);
        else
            writeOpOne("$min", field, val);
    }

    public void max(String field, int val, boolean multiple) {
        if (multiple)
            writeOp("$max", field, val);
        else
            writeOpOne("$max", field, val);
    }


    public void rename(String fieldOld, String fieldNew, boolean multiple) {
        if (multiple)
            writeOp("$rename", fieldOld, fieldNew);
        else
            writeOpOne("$rename", fieldOld, fieldNew);
    }

    public void dec(String field, boolean multiple) {
        dec(field, -1, multiple);
    }

    public void dec(String field, int amount, boolean multiple) {
        if (multiple)
            writeOp("$inc", field, -amount);
        else
            writeOpOne("$inc", field, -amount);
    }

    public void pull(String field, boolean multiple, Object... value) {
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
        if (multiple)
            writeOp("$pop", field, first ? -1 : 1);
        else
            writeOpOne("$pop", field, first ? -1 : 1);
    }

}
