package de.caluga.morphium.writer;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.bulk.BulkOperationContext;
import de.caluga.morphium.bulk.BulkRequestWrapper;
import de.caluga.morphium.query.Query;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.13
 * Time: 11:41
 * <p/>
 * Buffered Writer buffers all write requests (store, update, remove...) to mongo for a certain time. After that time the requests are
 * issued en block to mongo. Attention: this is not using BULK-Requests yet!
 */
@SuppressWarnings({"EmptyCatchBlock", "SynchronizeOnNonFinalField"})
public class BufferedMorphiumWriterImpl implements MorphiumWriter {

    private Morphium morphium;
    private MorphiumWriter directWriter;
    //needs to be securely stored
    private Map<Class<?>, List<WriteBufferEntry>> opLog = new ConcurrentHashMap<>(); //synced
    private Map<Class<?>, Long> lastRun = new ConcurrentHashMap<>();
    private Thread housekeeping;
    private boolean running = true;
    private static Logger logger = new Logger(BufferedMorphiumWriterImpl.class);
    private boolean orderedExecution = false;

    public BufferedMorphiumWriterImpl() {

    }

    public boolean isOrderedExecution() {
        return orderedExecution;
    }

    public void setOrderedExecution(boolean orderedExecution) {
        this.orderedExecution = orderedExecution;
    }

    private List<WriteBufferEntry> flushToQueue(List<WriteBufferEntry> localQueue) {
        //either buffer size reached, or time is up => queue writes
        List<WriteBufferEntry> didNotWrite = new ArrayList<WriteBufferEntry>();
        //queueing all ops in queue
        BulkOperationContext ctx = new BulkOperationContext(morphium, orderedExecution);

        for (WriteBufferEntry entry : localQueue) {
            try {
                entry.getToRun().exec(ctx);
                entry.getCb().onOperationSucceeded(entry.getType(), null, 0, null, null);
            } catch (RejectedExecutionException e) {
                logger.info("too much load - add write to next run");
                didNotWrite.add(entry);
            } catch (Exception e) {
                logger.error("could not write", e);
            }
        }
        localQueue = null; //let GC finish the work
        ctx.execute();
        return didNotWrite;
    }


    public void addToWriteQueue(Class<?> type, BufferedBulkOp r, AsyncOperationCallback c, AsyncOperationType t) {
        synchronized (opLog) {
            WriteBufferEntry wb = new WriteBufferEntry(r, System.currentTimeMillis(), c, t);
            if (opLog.get(type) == null) {
                opLog.put(type, new Vector<WriteBufferEntry>());
            }
            WriteBuffer w = morphium.getARHelper().getAnnotationFromHierarchy(type, WriteBuffer.class);
            int size = 0;
            int timeout = morphium.getConfig().getWriteBufferTime();
            WriteBuffer.STRATEGY strategy = WriteBuffer.STRATEGY.JUST_WARN;

            if (w != null) {
                size = w.size();
                timeout = w.timeout();
                strategy = w.strategy();
            }
            if (size > 0 && opLog.get(type).size() > size) {
                logger.warn("WARNING: Write buffer for type " + type.getName() + " maximum exceeded: " + opLog.get(type).size() + " entries now, max is " + size);
                BulkOperationContext ctx = new BulkOperationContext(morphium, orderedExecution);
                switch (strategy) {
                    case JUST_WARN:
                        opLog.get(type).add(wb);
                        break;
                    case IGNORE_NEW:
                        logger.warn("ignoring new incoming...");
                        return;
                    case WRITE_NEW:
                        logger.warn("directly writing data... due to strategy setting");
                        r.exec(ctx);
                        break;
                    case WRITE_OLD:

                        Collections.sort(opLog.get(type), new Comparator<WriteBufferEntry>() {
                            @Override
                            public int compare(WriteBufferEntry o1, WriteBufferEntry o2) {
                                return Long.valueOf(o1.getTimestamp()).compareTo(o2.getTimestamp());
                            }
                        });

                        opLog.get(type).get(0).getToRun().exec(ctx);
                        opLog.get(type).remove(0);
                        opLog.get(type).add(wb);
                        break;
                    case DEL_OLD:

                        Collections.sort(opLog.get(type), new Comparator<WriteBufferEntry>() {
                            @Override
                            public int compare(WriteBufferEntry o1, WriteBufferEntry o2) {
                                return Long.valueOf(o1.getTimestamp()).compareTo(o2.getTimestamp());
                            }
                        });
                        if (logger.isDebugEnabled()) {
                            logger.debug("Deleting oldest entry");
                        }
                        opLog.get(type).remove(0);
                        opLog.get(type).add(wb);
                        return;
                }
                ctx.execute();

            } else {
                opLog.get(type).add(wb);
            }
        }
    }

    @Override
    public <T> void store(final T o, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
                //do nothing
                boolean isNew = morphium.getARHelper().getId(o) == null;
                morphium.firePreStoreEvent(o, isNew);
                ctx.insert(o);
                morphium.getCache().clearCacheIfNecessary(o.getClass());
                morphium.firePostStoreEvent(o, isNew);
            }
        }, c, AsyncOperationType.WRITE);
    }

    @Override
    public <T> void store(final List<T> lst, AsyncOperationCallback<T> c) {
        if (lst == null || lst.size() == 0) {
            if (c != null)
                c.onOperationSucceeded(AsyncOperationType.WRITE, null, 0, lst, null);
            return;
        }
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);

        //TODO: think of something more accurate
        addToWriteQueue(lst.get(0).getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
                Map<Object, Boolean> map = new HashMap<Object, Boolean>();
                morphium.firePreStoreEvent(map);
                for (T o : lst) {
                    map.put(o, morphium.getARHelper().getId(o) == null);
                    ctx.insert(o);
                    morphium.getCache().clearCacheIfNecessary(o.getClass());

                }
                morphium.firePostStore(map);
            }
        }, c, AsyncOperationType.WRITE);
    }

    @Override
    public <T> void updateUsingFields(final T ent, final String collection, AsyncOperationCallback<T> c, final String... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(ent.getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.updateUsingFields(ent, collection, callback, fields);
                morphium.firePreUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);
                Query<Object> query = morphium.createQueryFor(ent.getClass()).f(morphium.getARHelper().getIdFieldName(ent)).eq(morphium.getARHelper().getId(ent));
                if (collection != null)
                    query.setCollectionName(collection);
                BulkRequestWrapper r = ctx.addFind(query);
                String[] flds = fields;
                if (flds.length == 0) {
                    flds = morphium.getARHelper().getAllFields(ent.getClass()).toArray(flds);
                }

                for (String f : flds) {
                    r.set(f, morphium.getARHelper().getValue(ent, f), false);
                }
                morphium.getCache().clearCacheIfNecessary(ent.getClass());
                morphium.firePostUpdateEvent(ent.getClass(), MorphiumStorageListener.UpdateTypes.SET);

            }
        }, c, AsyncOperationType.UPDATE);
    }

    @Override
    public <T> void set(final T toSet, final String collection, final String field, final Object value, final boolean insertIfNotExists, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(toSet.getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                morphium.firePreUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.SET);
                Query<Object> query = morphium.createQueryFor(toSet.getClass()).f(morphium.getARHelper().getIdFieldName(toSet)).eq(morphium.getARHelper().getId(toSet));
                if (collection != null) query.setCollectionName(collection);
                BulkRequestWrapper wr = ctx.addFind(query);
                if (insertIfNotExists) {
                    wr = wr.upsert();
                }
                morphium.getCache().clearCacheIfNecessary(toSet.getClass());
                wr.set(field, value, multiple);
                morphium.firePostUpdateEvent(toSet.getClass(), MorphiumStorageListener.UpdateTypes.SET);
            }
        }, c, AsyncOperationType.SET);
    }


    @Override
    public <T> void set(final Query<T> query, final Map<String, Object> values, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
                BulkRequestWrapper wr = ctx.addFind(query);
                if (insertIfNotExist) {
                    wr = wr.upsert();
                }
                morphium.getCache().clearCacheIfNecessary(query.getType());
                for (Map.Entry kv : values.entrySet()) {
                    wr.set(kv.getKey().toString(), kv.getValue(), multiple);
                }
                morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.SET);
            }
        }, c, AsyncOperationType.SET);
    }

    @Override
    public <T> void inc(final Query<T> query, final Map<String, Double> fieldsToInc, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
                BulkRequestWrapper wr = ctx.addFind(query);
                if (insertIfNotExist) {
                    wr = wr.upsert();
                }
                morphium.getCache().clearCacheIfNecessary(query.getType());
                for (Map.Entry kv : fieldsToInc.entrySet()) {
                    wr.inc(kv.getKey().toString(), ((Double) kv.getValue()).intValue(), multiple);
                }
                morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
            }
        }, c, AsyncOperationType.INC);
    }


    @Override
    public <T> void inc(final Query<T> query, final String field, final double amount, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                BulkRequestWrapper wr = ctx.addFind(query);
                morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
                if (insertIfNotExist) {
                    wr = wr.upsert();
                }
                morphium.getCache().clearCacheIfNecessary(query.getType());
                wr.inc(field, (int) amount, multiple);
                morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.INC);
            }
        }, c, AsyncOperationType.INC);
    }

    @Override
    public <T> void inc(final T obj, final String collection, final String field, final double amount, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);

        addToWriteQueue(obj.getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                morphium.firePreUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.INC);
                Query q = morphium.createQueryFor(obj.getClass()).f(morphium.getARHelper().getIdFieldName(obj)).eq(morphium.getARHelper().getId(obj));
                BulkRequestWrapper wr = ctx.addFind(q);
                morphium.getCache().clearCacheIfNecessary(obj.getClass());
                wr.inc(field, (int) amount, false);
                morphium.firePostUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.INC);
            }
        }, c, AsyncOperationType.INC);

    }


    @Override
    public <T> void pop(final T obj, final String collection, final String field, final boolean first, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                morphium.firePreUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.POP);
                Query q = morphium.createQueryFor(obj.getClass()).f(morphium.getARHelper().getIdFieldName(obj)).eq(morphium.getARHelper().getId(obj));
                BulkRequestWrapper wr = ctx.addFind(q);
                morphium.getCache().clearCacheIfNecessary(obj.getClass());
                wr.pop(field, first, false);
                morphium.firePostUpdateEvent(obj.getClass(), MorphiumStorageListener.UpdateTypes.POP);
            }
        }, c, AsyncOperationType.WRITE);
    }


    @Override
    public void setMorphium(Morphium m) {
        morphium = m;

        directWriter = m.getConfig().getWriter();

        housekeeping = new Thread() {
            @SuppressWarnings("SynchronizeOnNonFinalField")
            public void run() {
                while (running) {
                    try {
                        //processing and clearing write cache...
                        List<Class<?>> localBuffer = new ArrayList<Class<?>>();
                        synchronized (opLog) {
                            for (Class<?> clz : opLog.keySet()) {
                                localBuffer.add(clz);
                            }


                            for (Class<?> clz : localBuffer) {
                                if (opLog.get(clz) == null || opLog.get(clz).size() == 0) {
                                    continue;
                                }
                                WriteBuffer w = morphium.getARHelper().getAnnotationFromHierarchy(clz, WriteBuffer.class);
                                int size = 0;
                                int timeout = morphium.getConfig().getWriteBufferTime();
//                                WriteBuffer.STRATEGY strategy = WriteBuffer.STRATEGY.JUST_WARN;

                                if (w != null) {
                                    size = w.size();
                                    timeout = w.timeout();
//                                    strategy = w.strategy();
                                }
                                //can't be null
                                if (timeout == -1 && size > 0 && opLog.get(clz).size() < size) {
                                    continue; //wait for buffer to be filled
                                }

                                if (lastRun.get(clz) != null && System.currentTimeMillis() - lastRun.get(clz) < timeout) {
                                    //timeout not reached....
                                    continue;
                                }
                                lastRun.put(clz, System.currentTimeMillis());
                                List<WriteBufferEntry> localQueue;
                                localQueue = opLog.get(clz);
                                opLog.put(clz, new Vector<WriteBufferEntry>());

                                opLog.get(clz).addAll(flushToQueue(localQueue));
                                localQueue = null;
                            }


                        }
                    } catch (Exception e) {
                        logger.info("Got exception during write buffer handling!", e);
                    }

                    try {
                        if (morphium != null) {
                            if (morphium.getConfig() == null) {
                                running = false;
                                break;
                            }
                            Thread.sleep(morphium.getConfig().getWriteBufferTimeGranularity());
                        } else {
                            logger.warn("Morphium not set - assuming timeout of 1sec");
                            Thread.sleep(1000);
                        }
                    } catch (InterruptedException e) {
                    }
                }
            }


        };
        housekeeping.setDaemon(true);
        housekeeping.start();

    }

    @Override
    public <T> void remove(final List<T> lst, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        for (final T obj : lst) {
            addToWriteQueue(obj.getClass(), new BufferedBulkOp() {
                @Override
                public void exec(BulkOperationContext ctx) {
                    Query q = morphium.createQueryFor(obj.getClass()).f(morphium.getARHelper().getIdFieldName(obj)).eq(morphium.getARHelper().getId(obj));
                    morphium.firePreRemoveEvent(q);
                    BulkRequestWrapper r = ctx.addFind(q);
                    r.remove();
                    morphium.firePostRemoveEvent(q);
                }
            }, c, AsyncOperationType.REMOVE);
        }
    }

    @Override
    public <T> void remove(final T o, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
                Query q = morphium.createQueryFor(o.getClass()).f(morphium.getARHelper().getIdFieldName(o)).eq(morphium.getARHelper().getId(o));
                morphium.firePreRemoveEvent(q);
                BulkRequestWrapper r = ctx.addFind(q);
                r.remove();
                morphium.firePostRemoveEvent(q);
            }
        }, c, AsyncOperationType.REMOVE);
    }

    @Override
    public <T> void remove(final Query<T> q, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(q.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
                BulkRequestWrapper r = ctx.addFind(q);
                morphium.firePreRemoveEvent(q);
                morphium.getCache().clearCacheIfNecessary(q.getType());
                r.remove();
                morphium.firePostRemoveEvent(q);
            }
        }, c, AsyncOperationType.REMOVE);
    }

    @Override
    public <T> void pushPull(final boolean push, final Query<T> q, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(q.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.pushPull(push, query, field, value, insertIfNotExist, multiple, callback);
                BulkRequestWrapper r = ctx.addFind(q);
                if (insertIfNotExist) {
                    r = r.upsert();
                }
                morphium.getCache().clearCacheIfNecessary(q.getType());
                if (push) {
                    morphium.firePreUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    r.push(field, multiple, value);
                    morphium.firePostUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                } else {
                    morphium.firePreUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PULL);
                    r.pull(field, multiple, value);
                    morphium.firePostUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PULL);
                }
            }
        }, c, push ? AsyncOperationType.PUSH : AsyncOperationType.PULL);
    }

    @Override
    public <T> void pushPullAll(final boolean push, final Query<T> q, final String field, final List<?> value, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(q.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.pushPull(push, query, field, value, insertIfNotExist, multiple, callback);
                BulkRequestWrapper r = ctx.addFind(q);
                if (insertIfNotExist) {
                    r = r.upsert();
                }
                morphium.getCache().clearCacheIfNecessary(q.getType());
                if (push) {
                    morphium.firePreUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                    for (Object o : value) {
                        r.push(field, multiple, o);
                    }
                    morphium.firePostUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PUSH);
                } else {
                    morphium.firePreUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PULL);
                    for (Object o : value) {
                        r.pull(field, multiple, o);
                    }
                    morphium.firePostUpdateEvent(q.getType(), MorphiumStorageListener.UpdateTypes.PULL);
                }
            }
        }, c, push ? AsyncOperationType.PUSH : AsyncOperationType.PULL);
    }

    @Override
    public <T> void unset(final T obj, final String collection, final String field, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                Query q = morphium.createQueryFor(obj.getClass()).f(morphium.getARHelper().getIdFieldName(obj)).eq(morphium.getARHelper().getId(obj));
                if (collection != null) q.setCollectionName(collection);
                BulkRequestWrapper wr = ctx.addFind(q);
                morphium.getCache().clearCacheIfNecessary(obj.getClass());
                wr.unset(field, false);
            }
        }, c, AsyncOperationType.UNSET);
    }

    @Override
    public <T> void unset(final Query<T> query, final String field, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
                BulkRequestWrapper wr = ctx.addFind(query);
                wr.unset(field, multiple);
                morphium.getCache().clearCacheIfNecessary(query.getType());
                morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
            }
        }, c, AsyncOperationType.UNSET);
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> c, final boolean multiple, final String... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
                BulkRequestWrapper wr = ctx.addFind(query);
                for (String f : fields) {
                    wr.unset(f, multiple);
                }
                morphium.getCache().clearCacheIfNecessary(query.getType());
                morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
            }
        }, c, AsyncOperationType.UNSET);
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> c, final boolean multiple, final Enum... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
//                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
                morphium.firePreUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);

                BulkRequestWrapper wr = ctx.addFind(query);
                for (Enum f : fields) {
                    wr.unset(f.name(), multiple);
                }
                morphium.getCache().clearCacheIfNecessary(query.getType());
                morphium.firePostUpdateEvent(query.getType(), MorphiumStorageListener.UpdateTypes.UNSET);
            }
        }, c, AsyncOperationType.UNSET);
    }

    @Override
    public <T> void dropCollection(final Class<T> cls, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(cls, new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
                directWriter.dropCollection(cls, collection, callback);
                morphium.getCache().clearCacheIfNecessary(cls);
            }
        }, c, AsyncOperationType.REMOVE);
    }

    public <T> void ensureIndex(final Class<T> cls, final String collection, final Map<String, Object> index, AsyncOperationCallback<T> c) {
        ensureIndex(cls, collection, index, null, c);
    }

    @Override
    public <T> void ensureIndex(final Class<T> cls, final String collection, final Map<String, Object> index, final Map<String, Object> options, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(cls, new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
                directWriter.ensureIndex(cls, collection, index, options, callback);
            }
        }, c, AsyncOperationType.ENSURE_INDICES);
    }


    @Override
    public int writeBufferCount() {
        int cnt = 0;
        synchronized (opLog) {
            for (List<WriteBufferEntry> lst : opLog.values()) {
                cnt += lst.size();
            }
        }
        return cnt;
    }

    @Override
    public <T> void store(final List<T> lst, final String collectionName, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        if (lst == null || lst.size() == 0) return;
        addToWriteQueue(lst.get(0).getClass(), new BufferedBulkOp() {
            @Override
            public void exec(BulkOperationContext ctx) {
                Map<Object, Boolean> map = new HashMap<Object, Boolean>();
                for (T o : lst) {
                    ctx.insert(o);
                }
                morphium.firePreStoreEvent(map);
                for (T o : lst) {
                    map.put(o, morphium.getARHelper().getId(o) == null);
                    morphium.getCache().clearCacheIfNecessary(o.getClass());
                }
                morphium.firePostStore(map);
            }
        }, c, AsyncOperationType.WRITE);
    }

    @Override
    public void flush() {
        List<Class<?>> localBuffer = new ArrayList<Class<?>>();
        synchronized (opLog) {
            for (Class<?> clz : opLog.keySet()) {
                localBuffer.add(clz);
            }
            for (Class<?> clz : localBuffer) {
                if (opLog.get(clz) == null || opLog.get(clz).size() == 0) {
                    continue;
                }
                opLog.get(clz).addAll(flushToQueue(opLog.get(clz)));
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        logger.info("Stopping housekeeping thread");
        housekeeping.stop();
        super.finalize();
    }

    private class WriteBufferEntry {
        private BufferedBulkOp toRun;
        private AsyncOperationCallback cb;
        private AsyncOperationType type;
        private long timestamp;

        private WriteBufferEntry(BufferedBulkOp toRun, long timestamp, AsyncOperationCallback c, AsyncOperationType t) {
            this.toRun = toRun;
            this.timestamp = timestamp;
            this.cb = c;
            this.type = t;
        }

        public AsyncOperationType getType() {
            return type;
        }

        public void setType(AsyncOperationType type) {
            this.type = type;
        }

        public AsyncOperationCallback getCb() {
            return cb;
        }

        public void setCb(AsyncOperationCallback cb) {
            this.cb = cb;
        }

        public BufferedBulkOp getToRun() {
            return toRun;
        }

        public void setToRun(BufferedBulkOp toRun) {
            this.toRun = toRun;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    @Override
    public void setMaximumQueingTries(int n) {
        directWriter.setMaximumQueingTries(n);
    }

    @Override
    public void setPauseBetweenTries(int p) {
        directWriter.setPauseBetweenTries(p);
    }

    private class AsyncOpAdapter<T> implements AsyncOperationCallback<T> {

        @Override
        public void onOperationSucceeded(AsyncOperationType type, Query<T> q, long duration, List<T> result, T entity, Object... param) {
        }

        @Override
        public void onOperationError(AsyncOperationType type, Query<T> q, long duration, String error, Throwable t, T entity, Object... param) {

        }
    }

    private interface BufferedBulkOp {
        public void exec(BulkOperationContext ctx);
    }
}
