package de.caluga.morphium.writer;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.StatisticKeys;
import de.caluga.morphium.annotations.caching.WriteBuffer;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.RejectedExecutionException;

/**
 * User: Stephan Bösebeck
 * Date: 11.03.13
 * Time: 11:41
 * <p/>
 * TODO: Add documentation here
 */
@SuppressWarnings({"EmptyCatchBlock", "SynchronizeOnNonFinalField"})
public class BufferedMorphiumWriterImpl implements MorphiumWriter {

    private Morphium morphium;
    private AnnotationAndReflectionHelper annotationHelper = new AnnotationAndReflectionHelper();
    private MorphiumWriter directWriter;
    private Map<Class<?>, List<WriteBufferEntry>> opLog = new Hashtable<Class<?>, List<WriteBufferEntry>>(); //synced
    private Map<Class<?>, Long> lastRun = new Hashtable<Class<?>, Long>();
    private final Thread housekeeping;
    private boolean running = true;
    private Logger logger = Logger.getLogger(BufferedMorphiumWriterImpl.class);


    public BufferedMorphiumWriterImpl() {
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
                        }

                        for (Class<?> clz : localBuffer) {
                            synchronized (opLog) {
                                if (opLog.get(clz) == null || opLog.get(clz).size() == 0) {
                                    continue;
                                }
                            }
                            WriteBuffer wb = annotationHelper.getAnnotationFromHierarchy(clz, WriteBuffer.class);
                            //can't be null
                            if (wb.timeout() == -1 && wb.size() > 0 && opLog.get(clz).size() < wb.size()) {
                                continue; //wait for buffer to be filled
                            }
                            long timeout = morphium.getConfig().getWriteBufferTime();
                            if (wb.timeout() != 0) {
                                timeout = wb.timeout();
                            }
                            if (lastRun.get(clz) != null && System.currentTimeMillis() - lastRun.get(clz) < timeout) {
                                //timeout not reached....
                                continue;
                            }
                            lastRun.put(clz, System.currentTimeMillis());
                            //neither buffer size reached, or time is up => queue writes
                            List<WriteBufferEntry> localQueue;
                            synchronized (opLog) {
                                localQueue = opLog.get(clz);
                                opLog.put(clz, new Vector<WriteBufferEntry>());
                            }
                            //queueing all ops in queue
                            for (WriteBufferEntry entry : localQueue) {
                                waitForWriters();
                                try {
                                    entry.getToRun().run();
                                } catch (RejectedExecutionException e) {
                                    logger.info("too much load - add write to next run");
                                    opLog.get(clz).add(entry);
                                } catch (Exception e) {
                                    logger.error("could not write", e);
                                }
                            }
                            localQueue = null; //let GC finish the work
                        }
                    } catch (Exception e) {
                        logger.info("Got exception during write buffer handling!", e);
                    }

                    try {
                        Thread.sleep(morphium.getConfig().getWriteBufferTimeGranularity());
                    } catch (Exception e) {
                    }
                }
            }


        };
        housekeeping.setDaemon(true);
        housekeeping.start();
    }

    private void waitForWriters() {
        while (directWriter.writeBufferCount() > morphium.getConfig().getMaxConnections() * morphium.getConfig().getBlockingThreadsMultiplier() * 0.9 - 1) {
            try {

                if (logger.isDebugEnabled()) {
                    logger.debug("have to wait - maximum connection limit almost reached");
                }
                Thread.sleep(500); //wait for threads to finish
            } catch (InterruptedException e) {
            }
        }
    }

    public void addToWriteQueue(Class<?> type, Runnable r) {
        WriteBufferEntry wb = new WriteBufferEntry(r, System.currentTimeMillis());
        if (opLog.get(type) == null) {
            opLog.put(type, new Vector<WriteBufferEntry>());
        }
        WriteBuffer w = annotationHelper.getAnnotationFromHierarchy(type, WriteBuffer.class);
        if (w.size() > 0 && opLog.get(type).size() > w.size()) {
            logger.warn("WARNING: Write buffer maximum exceeded: " + opLog.get(type).size() + " entries now, max is " + w.size());
            switch (w.strategy()) {
                case JUST_WARN:
                    break;
                case IGNORE_NEW:
                    logger.warn("ignoring new incoming...");
                    return;
                case WRITE_NEW:
                    logger.warn("directly writing data... due to strategy setting");
                    r.run();
                    waitForWriters();
                    return;
                case WRITE_OLD:
                    synchronized (opLog) {
                        Collections.sort(opLog.get(type), new Comparator<WriteBufferEntry>() {
                            @Override
                            public int compare(WriteBufferEntry o1, WriteBufferEntry o2) {
                                return Long.valueOf(o1.getTimestamp()).compareTo(o2.getTimestamp());
                            }
                        });

                        for (int i = 0; i < opLog.get(type).size() - w.size(); i++) {
                            opLog.get(type).get(i).getToRun().run();
                            opLog.get(type).remove(i);
                            waitForWriters();
                        }
                    }
                    return;
                case DEL_OLD:
                    synchronized (opLog) {
                        Collections.sort(opLog.get(type), new Comparator<WriteBufferEntry>() {
                            @Override
                            public int compare(WriteBufferEntry o1, WriteBufferEntry o2) {
                                return Long.valueOf(o1.getTimestamp()).compareTo(o2.getTimestamp());
                            }
                        });

                        for (int i = 0; i < opLog.get(type).size() - w.size(); i++) {
                            opLog.get(type).get(i).getToRun().run();
                            opLog.get(type).remove(i);
                        }
                    }
                    return;
            }

        }
        opLog.get(type).add(wb);

    }

    @Override
    public <T> void store(final T o, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.store(o, collection, callback);
            }
        });
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
        addToWriteQueue(lst.get(0).getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.store(lst, callback);
            }
        });
    }

    @Override
    public <T> void updateUsingFields(final T ent, final String collection, AsyncOperationCallback<T> c, final String... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(ent.getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.updateUsingFields(ent, collection, callback, fields);
            }
        });
    }

    @Override
    public <T> void set(final T toSet, final String collection, final String field, final Object value, final boolean insertIfNotExists, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(toSet.getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.set(toSet, collection, field, value, insertIfNotExists, multiple, callback);
            }
        });
    }


    @Override
    public <T> void set(final Query<T> query, final Map<String, Object> values, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.set(query, values, insertIfNotExist, multiple, callback);
            }
        });
    }

    @Override
    public <T> void inc(final Query<T> query, final String field, final int amount, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.inc(query, field, amount, insertIfNotExist, multiple, callback);
            }
        });
    }

    @Override
    public <T> void inc(final T toInc, final String collection, final String field, final int amount, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(toInc.getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.inc(toInc, collection, field, amount, callback);
            }
        });
    }


    @Override
    public void setMorphium(Morphium m) {
        morphium = m;
        annotationHelper = m.getARHelper();
        directWriter = m.getConfig().getWriter();
    }

    @Override
    public <T> void delete(final List<T> lst, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(lst.get(0).getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.delete(lst, callback);
            }
        });
    }

    @Override
    public <T> void delete(final T o, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.delete(o, collection, callback);
            }
        });
    }

    @Override
    public <T> void delete(final Query<T> q, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(q.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.delete(q, callback);
            }
        });
    }

    @Override
    public <T> void pushPull(final boolean push, final Query<T> query, final String field, final Object value, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.pushPull(push, query, field, value, insertIfNotExist, multiple, callback);
            }
        });
    }

    @Override
    public <T> void pushPullAll(final boolean push, final Query<T> query, final String field, final List<?> value, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.pushPullAll(push, query, field, value, insertIfNotExist, multiple, callback);
            }
        });
    }

    @Override
    public <T> void unset(final T toSet, final String collection, final String field, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(toSet.getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.unset(toSet, collection, field, callback);
            }
        });
    }

    @Override
    public <T> void dropCollection(final Class<T> cls, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(cls, new Runnable() {
            @Override
            public void run() {
                directWriter.dropCollection(cls, collection, callback);
            }
        });
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
        addToWriteQueue(cls, new Runnable() {
            @Override
            public void run() {
                directWriter.ensureIndex(cls, collection, index, options, callback);
            }
        });
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


    private class WriteBufferEntry {
        private Runnable toRun;
        private long timestamp;

        private WriteBufferEntry(Runnable toRun, long timestamp) {
            this.toRun = toRun;
            this.timestamp = timestamp;
        }

        public Runnable getToRun() {
            return toRun;
        }

        public void setToRun(Runnable toRun) {
            this.toRun = toRun;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    private class AsyncOpAdapter<T> implements AsyncOperationCallback<T> {

        @Override
        public void onOperationSucceeded(AsyncOperationType type, Query<T> q, long duration, List<T> result, T entity, Object... param) {
        }

        @Override
        public void onOperationError(AsyncOperationType type, Query<T> q, long duration, String error, Throwable t, T entity, Object... param) {

        }
    }
}
