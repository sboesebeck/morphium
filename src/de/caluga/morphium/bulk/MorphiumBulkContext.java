package de.caluga.morphium.bulk;/**
 * Created by stephan on 18.11.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.Utils;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.bulk.DeleteBulkRequest;
import de.caluga.morphium.driver.bulk.UpdateBulkRequest;
import de.caluga.morphium.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * context for doing bulk operations. What it does is, it stores all operations here and will send them to mongodb en block
 **/
public class MorphiumBulkContext<T> {
    private final Logger log = new Logger(MorphiumBulkContext.class);
    private final BulkRequestContext ctx;

    private final List<Runnable> preEvents = new ArrayList<>();
    private final List<Runnable> postEvents = new ArrayList<>();

    public MorphiumBulkContext(BulkRequestContext ctx) {
        this.ctx = ctx;
    }


    public Map<String, Object> runBulk() {
        firePre();
        Map<String, Object> ret;
        try {
            ret = ctx.execute();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
        firePost();
        return ret;
    }

    public int getNumberOfRequests() {
        return preEvents.size();
    }

    private void firePre() {
        preEvents.forEach(Runnable::run);
    }

    private void firePost() {
        postEvents.forEach(Runnable::run);
    }

    @SuppressWarnings("unused")
    public void runBulk(AsyncOperationCallback c) {
        if (c == null) {
            firePre();
            try {
                ctx.execute();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
            firePost();
        } else {
            new Thread() {
                @Override
                public void run() {
                    firePre();
                    try {
                        Map<String, Object> ret = ctx.execute();
                        //noinspection unchecked
                        c.onOperationSucceeded(AsyncOperationType.BULK, null, 0, null, null, ret);
                    } catch (Exception e) {
                        //noinspection unchecked
                        c.onOperationError(AsyncOperationType.BULK, null, 0, null, e, null);
                    }
                    firePost();
                }
            }.start();
        }
    }

    private void createUpdateRequest(Query<T> query, String command, Map values, boolean upsert, boolean multiple) {
        UpdateBulkRequest up = ctx.addUpdateBulkRequest();
        up.setQuery(query.toQueryObject());
        up.setUpsert(upsert);
        up.setCmd(Utils.getMap(command, values));
        up.setMultiple(multiple);

        preEvents.add(() -> {
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
        });

        postEvents.add(() -> {
            switch (command) {
                case "$set":
                    ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
                    break;
                case "$inc":
                    ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
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
        preEvents.add(() -> {
            if (toInsert.size() == 1) {
                ctx.getMorphium().firePreStore(toInsert.get(0), ctx.getMorphium().getARHelper().getId(toInsert.get(0)) == null);
            } else {
                ctx.getMorphium().firePreStore(isNew);
            }
        });

        postEvents.add(() -> {
            if (toInsert.size() == 1) {
                ctx.getMorphium().firePostStore(toInsert.get(0), ctx.getMorphium().getARHelper().getId(toInsert.get(0)) == null);
            } else {
                ctx.getMorphium().firePostStore(isNew);
            }
        });


    }


    public void addDeleteRequest(T entity) {
        DeleteBulkRequest del = ctx.addDeleteBulkRequest();
        del.setQuery(Utils.getMap("_id", ctx.getMorphium().getARHelper().getId(entity)));
        del.setMultiple(false);
        preEvents.add(() -> ctx.getMorphium().firePreRemove(entity));

        postEvents.add(() -> ctx.getMorphium().firePostRemoveEvent(entity));

    }

    public void addDeleteRequest(List<T> entities) {
        entities.forEach(this::addDeleteRequest);

    }

    public void addDeleteRequest(Query<T> q, boolean multiple) {
        DeleteBulkRequest del = ctx.addDeleteBulkRequest();
        del.setMultiple(multiple);
        del.setQuery(q.toQueryObject());

        preEvents.add(() -> ctx.getMorphium().firePreRemoveEvent(q));

        postEvents.add(() -> ctx.getMorphium().firePostRemoveEvent(q));
    }


    public void addCustomUpdateRequest(Query<T> query, Map<String, Object> command, boolean upsert, boolean multiple) {
        UpdateBulkRequest up = ctx.addUpdateBulkRequest();
        up.setQuery(query.toQueryObject());
        up.setUpsert(upsert);
        up.setMultiple(multiple);
        up.setCmd(command);
        preEvents.add(() -> ctx.getMorphium().firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.CUSTOM));

        postEvents.add(() -> ctx.getMorphium().firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.CUSTOM));
    }

    @SuppressWarnings("unused")
    public void addSetRequest(T obj, String field, Object value, boolean upsert) {
        //noinspection unchecked
        addSetRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    @SuppressWarnings("unused")
    public void addUnSetRequest(T obj, String field, Object value, boolean upsert) {
        //noinspection unchecked
        addUnsetRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addSetRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$set", Utils.getMap(field, value), upsert, multiple);
    }

    public void addUnsetRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$unset", Utils.getMap(field, value), upsert, multiple);
    }

    public void addIncRequest(Query<T> query, String field, Number value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$inc", Utils.getMap(field, value), upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void addIncRequest(T obj, String field, Number value, boolean upsert) {
        //noinspection unchecked
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
        //noinspection unchecked
        addCurrentDateRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), upsert, false, field);
    }

    public void addMinRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$min", Utils.getMap(field, value), upsert, multiple);
    }

    public void addMinRequest(Query<T> query, Map<String, Object> toSet, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$min", toSet, upsert, multiple);
    }

    public void addMinRequest(T obj, String field, Object value, boolean upsert) {
        //noinspection unchecked
        addMinRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addMaxRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$max", Utils.getMap(field, value), upsert, multiple);
    }

    public void addMaxRequest(Query<T> query, Map<String, Object> toSet, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$max", toSet, upsert, multiple);
    }

    public void addMaxRequest(T obj, String field, Object value, boolean upsert) {
        //noinspection unchecked
        addMaxRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addRenameRequest(Query<T> query, String field, String newName, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$rename", Utils.getMap(field, newName), upsert, multiple);
    }

    public void addRenameRequest(T obj, String field, String newName, boolean upsert) {
        //noinspection unchecked
        addRenameRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, newName, upsert, false);
    }

    public void addMulRequest(Query<T> query, String field, Number value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$mul", Utils.getMap(field, value), upsert, multiple);
    }

    public void addMulRequest(T obj, String field, Number value, boolean upsert) {
        //noinspection unchecked
        addMulRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }

    public void addPopRequest(Query<T> query, String field, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$pop", Utils.getMap(field, 1), upsert, multiple);
    }

    public void addPopRequest(T obj, String field, boolean upsert) {
        //noinspection unchecked
        addPopRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, upsert, false);
    }

    public void addPushRequest(Query<T> query, String field, Object value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$push", Utils.getMap(field, value), upsert, multiple);
    }

    public void addPushRequest(T obj, String field, Object value, boolean upsert) {
        //noinspection unchecked
        addPushRequest(ctx.getMorphium().createQueryFor((Class<T>) obj.getClass()).f(ctx.getMorphium().getARHelper().getIdFieldName(obj)).eq(ctx.getMorphium().getARHelper().getId(obj)), field, value, upsert, false);
    }


    public void addSetRequest(Query<T> query, Map<String, Object> toSet, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$set", toSet, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void addUnsetRequest(Query<T> query, Map<String, Object> toSet, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$unset", toSet, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void addIncRequest(Query<T> query, Map<String, Number> toInc, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$inc", toInc, upsert, multiple);
    }

    @SuppressWarnings("unused")
    public void addPushRequest(Query<T> query, String field, List<Object> value, boolean upsert, boolean multiple) {
        createUpdateRequest(query, "$push", Utils.getMap(field, value), upsert, multiple);
    }


}
