package de.caluga.morphium.bulk;/**
 * Created by stephan on 18.11.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.bulk.DeleteBulkRequest;
import de.caluga.morphium.driver.bulk.UpdateBulkRequest;
import de.caluga.morphium.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class MorphiumBulkContext<T> {
    private Logger log = new Logger(MorphiumBulkContext.class);
    private BulkRequestContext ctx;

    private List<Runnable> preEvents = new ArrayList<>();
    private List<Runnable> postEvents = new ArrayList<>();

    public MorphiumBulkContext(BulkRequestContext ctx) {
        this.ctx = ctx;
    }


    public Map<String, Object> runBulk() {
        firePre();
        Map<String, Object> ret = ctx.execute();
        firePost();
        return ret;
    }

    public int getNumberOfRequests() {
        return preEvents.size();
    }

    private void firePre() {
        for (Runnable r : preEvents) {
            r.run();
        }
    }

    private void firePost() {
        for (Runnable r : postEvents) {
            r.run();
        }
    }

    public void runBulk(AsyncOperationCallback c) {
        if (c == null) {
            firePre();
            ctx.execute();
            firePost();
        } else {
            new Thread() {
                public void run() {
                    firePre();
                    try {
                        Map<String, Object> ret = ctx.execute();
                        c.onOperationSucceeded(AsyncOperationType.BULK, null, 0, null, null, ret);
                    } catch (Exception e) {
                        c.onOperationError(AsyncOperationType.BULK, null, 0, null, e, null);
                    }
                    firePost();
                }
            }.start();
        }
    }

    private <T> void createUpdateRequest(Query<T> query, String command, Map values, boolean upsert, boolean multiple) {
        UpdateBulkRequest up = ctx.addUpdateBulkRequest();
        up.setQuery(query.toQueryObject());
        up.setUpsert(upsert);
        up.setCmd(ctx.getMorphium().getMap(command, values));
        up.setMultiple(multiple);

        preEvents.add(new Runnable() {
            @Override
            public void run() {
                switch (command) {
                    case "$set":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
                        break;
                    case "$inc":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
                        break;
                    case "$unset":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
                        break;
                    case "$min":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.MIN);
                        break;
                    case "$max":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.MAX);
                        break;
                    case "$currentDate":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.CURRENTDATE);
                        break;

                    case "$pop":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.POP);
                        break;

                    case "$push":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                        break;

                    case "$mul":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.MUL);
                        break;

                    case "$rename":
                        ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.RENAME);
                        break;
                    default:
                        log.error("Unknown update command " + command);
                }
            }
        });

        postEvents.add(new Runnable() {
            @Override
            public void run() {
                switch (command) {
                    case "$set":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
                        break;
                    case "$inc":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
                        break;
                    case "$unset":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
                        break;
                    case "$min":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.MIN);
                        break;
                    case "$max":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.MAX);
                        break;
                    case "$currentDate":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.CURRENTDATE);
                        break;

                    case "$pop":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.POP);
                        break;

                    case "$push":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                        break;

                    case "$mul":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.MUL);
                        break;

                    case "$rename":
                        ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.RENAME);
                        break;
                    default:
                        log.error("Unknown update command " + command);
                }
            }
        });
    }

    public void addInsertRequest(List<T> toInsert) {
        Map<Object, Boolean> isNew = new HashMap<>();
        List<Map<String, Object>> ins = new ArrayList<>();
        for (Object o : toInsert) {
            Map<String, Object> marshall = ctx.getMorphium().getMapper().marshall(o);
            ins.add(marshall);
            isNew.put(o, marshall.get("_id") == null);
        }

        ctx.addInsertBulkReqpest(ins);
        preEvents.add(new Runnable() {
            @Override
            public void run() {
                if (toInsert.size() == 1) {
                    ctx.getMorphium().firePreStore(toInsert.get(0), ctx.getMorphium().getARHelper().getId(toInsert.get(0)) == null);
                } else {
                    ctx.getMorphium().firePreStore(isNew);
                }
            }
        });

        postEvents.add(new Runnable() {
            @Override
            public void run() {
                if (toInsert.size() == 1) {
                    ctx.getMorphium().firePostStore(toInsert.get(0), ctx.getMorphium().getARHelper().getId(toInsert.get(0)) == null);
                } else {
                    ctx.getMorphium().firePostStore(isNew);
                }
            }
        });


    }


    public void addDeleteRequest(T entity) {
        DeleteBulkRequest del = ctx.addDeleteBulkRequest();
        del.setQuery(ctx.getMorphium().getMap("_id", ctx.getMorphium().getARHelper().getId(entity)));
        del.setMultiple(false);
        preEvents.add(new Runnable() {
            @Override
            public void run() {
                ctx.getMorphium().firePreRemove(entity);
            }
        });

        postEvents.add(new Runnable() {
            @Override
            public void run() {
                ctx.getMorphium().firePostRemoveEvent(entity);
            }
        });

    }

    public void addDeleteRequest(List<T> entities) {
        for (T e : entities) {
            addDeleteRequest(e);
        }

    }

    public void addDeleteRequest(Query<T> q, boolean multiple) {
        DeleteBulkRequest del = ctx.addDeleteBulkRequest();
        del.setMultiple(multiple);
        del.setQuery(q.toQueryObject());

        preEvents.add(new Runnable() {
            @Override
            public void run() {
                ctx.getMorphium().firePreRemoveEvent(q);
            }
        });

        postEvents.add(new Runnable() {
            @Override
            public void run() {
                ctx.getMorphium().firePostRemoveEvent(q);
            }
        });
    }


    public void addCustomUpdateRequest(Query<T> query, Map<String, Object> command, boolean upsert, boolean multiple) {
        UpdateBulkRequest up = ctx.addUpdateBulkRequest();
        up.setQuery(query.toQueryObject());
        up.setUpsert(upsert);
        up.setMultiple(multiple);
        up.setCmd(command);
    }

    public void addSetRequest(T obj, String field, Object value, boolean upsert) {
        addSetRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addUnSetRequest(T obj, String field, Object value, boolean upsert) {
        addUnsetRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addSetRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$set", ctx.getMorphium().getMap(field, value), upsert, multiple);
    }

    public void addUnsetRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$unset", ctx.getMorphium().getMap(field, value), upsert, multiple);
    }

    public void addIncRequest(Query<T> query, String field, Number value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$inc", ctx.getMorphium().getMap(field, value), upsert, multiple);
    }

    public void addIncRequest(T obj, String field, Number value, boolean upsert) {
        addIncRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addCurrentDateRequest(Query<T> query, boolean upsert, boolean multiple, String... fld) {
        Map<String, Object> toSet = new HashMap<>();
        for (String f : fld) {
            toSet.put(f, true);
        }
        createUpdateRequest(query, "$currentDate", toSet, upsert, multiple);
    }

    public void addCurrentDateRequest(T obj, String field, boolean upsert) {
        addCurrentDateRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), upsert, false, field);
    }

    public void addMinRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$min", ctx.getMorphium().getMap(field, value), upsert, multiple);
    }

    public void addMinRequest(Query<T> query, Map<String, Object> toSet, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$min", toSet, upsert, multiple);
    }

    public void addMinRequest(T obj, String field, Object value, boolean upsert) {
        addMinRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addMaxRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$max", ctx.getMorphium().getMap(field, value), upsert, multiple);
    }

    public void addMaxRequest(Query<T> query, Map<String, Object> toSet, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$max", toSet, upsert, multiple);
    }

    public void addMaxRequest(T obj, String field, Object value, boolean upsert) {
        addMaxRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addRenameRequest(Query<T> query, String field, String newName, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$rename", ctx.getMorphium().getMap(field, newName), upsert, multiple);
    }

    public void addRenameRequest(T obj, String field, String newName, boolean upsert) {
        addRenameRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, newName, upsert, false);
    }

    public void addMulRequest(Query<T> query, String field, Number value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$mul", ctx.getMorphium().getMap(field, value), upsert, multiple);
    }

    public void addMulRequest(T obj, String field, Number value, boolean upsert) {
        addMulRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addPopRequest(Query<T> query, String field, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$pop", ctx.getMorphium().getMap(field, 1), upsert, multiple);
    }

    public void addPopRequest(T obj, String field, boolean upsert) {
        addPopRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, upsert, false);
    }

    public void addPushRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$push", ctx.getMorphium().getMap(field, value), upsert, multiple);
    }

    public void addPushRequest(T obj, String field, Object value, boolean upsert) {
        addPushRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }


    public void addSetRequest(Query<T> query, Map<String, Object> toSet, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$set", toSet, upsert, multiple);
    }

    public void addUnsetRequest(Query<T> query, Map<String, Object> toSet, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$unset", toSet, upsert, multiple);
    }

    public void addIncRequest(Query<T> query, Map<String, Number> toInc, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$inc", toInc, upsert, multiple);
    }

    public void addPushRequest(Query<T> query, String field, List<Object> value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$push", ctx.getMorphium().getMap(field, value), upsert, multiple);
    }


}
