package de.caluga.morphium.writer;

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
    private MorphiumWriter directWriter;
    private Map<Class<?>, List<WriteBufferEntry>> opLog = new Hashtable<Class<?>, List<WriteBufferEntry>>(); //synced
    private Map<Class<?>, Long> lastRun = new Hashtable<Class<?>, Long>();
    private Thread housekeeping;
    private boolean running = true;
    private static Logger logger = Logger.getLogger(BufferedMorphiumWriterImpl.class);


    public BufferedMorphiumWriterImpl() {

    }

    private List<WriteBufferEntry> flushToQueue(List<WriteBufferEntry> localQueue) {
        //either buffer size reached, or time is up => queue writes
        List<WriteBufferEntry> didNotWrite = new ArrayList<WriteBufferEntry>();
        //queueing all ops in queue
        for (WriteBufferEntry entry : localQueue) {
            try {
                entry.getToRun().run();
            } catch (RejectedExecutionException e) {
                logger.info("too much load - add write to next run");
                didNotWrite.add(entry);
            } catch (Exception e) {
                logger.error("could not write", e);
            }
        }
        localQueue = null; //let GC finish the work
        return didNotWrite;
    }


    public void addToWriteQueue(Class<?> type, Runnable r) {
        synchronized (opLog) {
            WriteBufferEntry wb = new WriteBufferEntry(r, System.currentTimeMillis());
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
                logger.warn("WARNING: Write buffer for type "+type.getName()+" maximum exceeded: " + opLog.get(type).size() + " entries now, max is " + size);
                switch (strategy) {
                    case JUST_WARN:
                        break;
                    case IGNORE_NEW:
                        logger.warn("ignoring new incoming...");
                        return;
                    case WRITE_NEW:
                        logger.warn("directly writing data... due to strategy setting");
                        r.run();
                        return;
                    case WRITE_OLD:

                        Collections.sort(opLog.get(type), new Comparator<WriteBufferEntry>() {
                            @Override
                            public int compare(WriteBufferEntry o1, WriteBufferEntry o2) {
                                return Long.valueOf(o1.getTimestamp()).compareTo(o2.getTimestamp());
                            }
                        });

                        opLog.get(type).get(0).getToRun().run();
                        opLog.get(type).remove(0);
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
                        break;
                }

            }
            opLog.get(type).add(wb);
        }
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
    public <T> void inc(final Query<T> query, final Map<String, Double> fieldsToInc, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.inc(query, fieldsToInc, insertIfNotExist, multiple, callback);
            }
        });
    }


    @Override
    public <T> void inc(final Query<T> query, final String field, final double amount, final boolean insertIfNotExist, final boolean multiple, AsyncOperationCallback<T> c) {
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
    public <T> void inc(final T toInc, final String collection, final String field, final double amount, AsyncOperationCallback<T> c) {
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
    public <T> void pop(final T obj, final String collection, final String field, final boolean first, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(obj.getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.pop(obj, collection, field, first, callback);
            }
        });
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
                                WriteBuffer.STRATEGY strategy = WriteBuffer.STRATEGY.JUST_WARN;

                                if (w != null) {
                                    size = w.size();
                                    timeout = w.timeout();
                                    strategy = w.strategy();
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
        addToWriteQueue(lst.get(0).getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.remove(lst, callback);
            }
        });
    }

    @Override
    public <T> void remove(final T o, final String collection, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(o.getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.remove(o, collection, callback);
            }
        });
    }

    @Override
    public <T> void remove(final Query<T> q, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(q.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.remove(q, callback);
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
    public <T> void unset(final Query<T> query, final String field, final boolean multiple, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.unset(query, field, multiple, callback);
            }
        });
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> c, final boolean multiple, final String... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.unset(query, callback, multiple, fields);
            }
        });
    }

    @Override
    public <T> void unset(final Query<T> query, AsyncOperationCallback<T> c, final boolean multiple, final Enum... fields) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        addToWriteQueue(query.getType(), new Runnable() {
            @Override
            public void run() {
                directWriter.unset(query, callback, multiple, fields);
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

    @Override
    public <T> void store(final List<T> lst, final String collectionName, AsyncOperationCallback<T> c) {
        if (c == null) {
            c = new AsyncOpAdapter<T>();
        }
        final AsyncOperationCallback<T> callback = c;
        morphium.inc(StatisticKeys.WRITES_CACHED);
        if (lst == null || lst.size() == 0) return;
        addToWriteQueue(lst.get(0).getClass(), new Runnable() {
            @Override
            public void run() {
                directWriter.store(lst, collectionName, callback);
            }
        });
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
}
