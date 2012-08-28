package de.caluga.morphium;

import org.apache.log4j.Logger;
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
 * TODO: Add documentation here
 */
public class SequenceGenerator {
    private int inc;
    private long startValue;
    private Morphium morphium;
    private String id;
    private String name;

    private static Logger log = Logger.getLogger(SequenceGenerator.class);

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

        if (!morphium.getDatabase().collectionExists(morphium.getConfig().getMapper().getCollectionName(Sequence.class)) || morphium.createQueryFor(Sequence.class).f(Sequence.PROPERTYNAME_NAME).eq(name).countAll() == 0) {
            //sequence does not exist yet
            if (log.isDebugEnabled()) log.debug("Sequence does not exist yet... inserting");
//            Query<Sequence> seq = morphium.createQueryFor(Sequence.class);
//            seq.f(Sequence.PROPERTYNAME_NAME).eq(name);
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
    }

    public long getCurrentValue() {
        Sequence s = (Sequence) morphium.createQueryFor(Sequence.class).f("name").eq(name).get();
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
        if (recLevel > 10) {
            log.error("Could not get lock on Sequence " + name);
            throw new RuntimeException("Getting lock on sequence " + name + " failed!");
        }
        Query<Sequence> seq = morphium.createQueryFor(Sequence.class);
        seq = seq.f(Sequence.PROPERTYNAME_NAME).eq(name);
        if (seq.countAll() == 0) {
            log.error("Sequence vanished?");
            throw new RuntimeException("Sequence vanished");
        } else {
            seq = morphium.createQueryFor(Sequence.class);
            seq.f(Sequence.PROPERTYNAME_NAME).eq(name).f(Sequence.PROPERTYNAME_LOCKED_BY).eq(null);

            Map<String, Object> values = new HashMap<String, Object>();
            values.put("locked_by", id);
            morphium.set(seq, values, false, false);
            if (log.isDebugEnabled()) {
                log.debug("loced sequence entry");
            }
        }
        seq = morphium.createQueryFor(Sequence.class);
        seq.f(Sequence.PROPERTYNAME_NAME).eq(name).f(Sequence.PROPERTYNAME_LOCKED_BY).eq(id);

        if (seq.countAll() == 0) {
            //locking failed... wait a moment, try again
//            if (log.isDebugEnabled()) {
            log.warn("Locking failed - recursing");
//            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
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
        morphium.inc(seq, Sequence.PROPERTYNAME_CURRENT_VALUE, inc);
        if (log.isDebugEnabled()) log.debug("increased it");
        Sequence s = seq.get();
        if (s == null) {
            log.error("locked Sequence not found anymore?");
            seq = MorphiumSingleton.get().createQueryFor(Sequence.class).f(Sequence.PROPERTYNAME_NAME).eq(name);
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
