package de.caluga.morphium;

import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * User: Stephan Bösebeck
 * Date: 24.07.12
 * Time: 21:36
 * <p/>
 * Generate a new unique sequence number. Uses Sequence to store the current value. Process is as follows:
 * <ul>
 * <li>lock the entry in sequence collection with my own UUID (rather unique) using atomar $set operation</li>
 * <li>read my locked entries (should only be one). If one is found, I have a valid lock. If not found, somebody
 * locked over. Wait a while and try again
 * </li>
 * <li>increase the number by one (or the given increment) on the locked element, get this value</li>
 * <li>remove the lock (atomar using $set)</li>
 * <li>return current value</li>
 * </ul>
 */

@SuppressWarnings("UnusedDeclaration")
public class SequenceGenerator {
    private int inc;
    private long startValue;
    private Morphium morphium;
    private String id;
    private String name;

    private static Logger log = new Logger(SequenceGenerator.class);

    public SequenceGenerator() {
        this(MorphiumSingleton.get(), "seq", 1, 1);
    }

    public SequenceGenerator(Morphium m, String n) {
        this(m, n, 1, 1);
    }

    public SequenceGenerator(Morphium mrph, String name, int inc, long startValue) {
        this.inc = inc;
        if (inc == 0) {
            throw new IllegalArgumentException("Cannot use increment value 0!");
        }
        this.name = name;
        this.startValue = startValue;
        this.morphium = mrph;
        id = UUID.randomUUID().toString();

        for (int i = 0; i < morphium.getConfig().getRetriesOnNetworkError(); i++) {
            try {
                if (!morphium.getDatabase().collectionExists(morphium.getMapper().getCollectionName(Sequence.class)) || morphium.createQueryFor(Sequence.class).f("name").eq(name).countAll() == 0) {
                    //sequence does not exist yet
                    if (log.isDebugEnabled()) log.debug("Sequence does not exist yet... inserting");
//            Query<Sequence> seq = morphium.createQueryFor(Sequence.class);
//            seq.f("name").eq(name);
//
//            Map<String, Object> values = new HashMap<String, Object>();
//            values.put("locked_by", null);
//            values.put("current_value", startValue - inc);
//            morphium.set(seq, values, true, false);
//            morphium.ensureIndicesFor(Sequence.class);

                    Sequence s = new Sequence();
                    s.setCurrentValue(startValue - inc);
                    s.setName(name);
                    s.setId(new ObjectId(new Date(0l), name.hashCode()));
                    morphium.storeNoCache(s);
                    //inserted
                }
                break;
            } catch (Throwable t) {
                morphium.handleNetworkError(i, t);
            }
        }
    }

    public long getCurrentValue() {
        Sequence s = morphium.createQueryFor(Sequence.class).f("name").eq(name).get();
        if (s == null || s.getCurrentValue() == null) {
            //new sequence - get default
            return getNextValue(1);
        }
        return s.getCurrentValue();
    }

    public long getNextValue() {
        return getNextValue(0);
    }

    private synchronized long getNextValue(int recLevel) {
        Query<Sequence> seq = morphium.createQueryFor(Sequence.class).f("name").eq(name);
        if (seq.countAll() == 0) {
            log.error("Sequence vanished?");
            throw new RuntimeException("Sequence vanished");
        }
        Map<String, Object> values = new HashMap<String, Object>();
        Sequence sequence = seq.get();
        if (recLevel > 30) {
            log.error("Could not get lock on Sequence " + name + " Checking timestamp...");
            if (System.currentTimeMillis() - sequence.getLockedAt() > 1000 * 30) {
                log.error("Was locked for more than 30s - assuming error, resetting lock");
                sequence.setLockedBy(null);
                morphium.store(sequence);
                if (log.isDebugEnabled()) {
                    log.debug("overwriting lock for locked sequence " + name);
                }
                return getNextValue(1);
            }
            throw new RuntimeException("Getting lock on sequence " + name + " failed!");
        }

        seq = seq.q().f("name").eq(name).f("locked_by").eq(null);
        values.put("locked_by", id);
        values.put("locked_at", System.currentTimeMillis());
        morphium.set(seq, values, false, false);
        if (log.isDebugEnabled()) {
            log.debug("locked sequence entry");
        }
        seq = morphium.createQueryFor(Sequence.class);
        seq.f("name").eq(name).f("locked_by").eq(id);

        if (seq.countAll() == 0) {
            //locking failed... wait a moment, try again
//            if (log.isDebugEnabled()) {
            log.warn("Locking failed - recursing");
//            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
            return getNextValue(recLevel + 1);
        }
        if (seq.countAll() > 1) {
            log.error("sequence name / locked by not unique??? - using first");
        }
        if (log.isDebugEnabled()) {
            log.debug("Found it!");
        }

//        Map<String, Object> values = new HashMap<String, Object>();
        morphium.inc(seq, "current_value", inc);
        if (log.isDebugEnabled()) log.debug("increased it");
        Sequence s = seq.get();
        if (s == null) {
            log.error("locked Sequence not found anymore?");
            seq = MorphiumSingleton.get().createQueryFor(Sequence.class).f("name").eq(name);
            for (Sequence sq : seq.asList()) {
                log.error("Sequence: " + sq.toString());
            }
            return -1;
        }
        s.setLockedBy(null);
        morphium.store(s);
        if (log.isDebugEnabled()) log.debug("unlocked it");

        return s.getCurrentValue();

    }

    public int getInc() {
        return inc;
    }

    public void setInc(int inc) {
        this.inc = inc;
    }

    public long getStartValue() {
        return startValue;
    }

    public void setStartValue(long startValue) {
        this.startValue = startValue;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Morphium getMorphium() {
        return morphium;
    }

    public void setMorphium(Morphium morphium) {
        this.morphium = morphium;
    }
}
