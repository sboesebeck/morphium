package de.caluga.morphium;

import de.caluga.morphium.cache.CacheElement;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 31.03.12
 * Time: 18:28
 * <p/>
 * TODO: Add documentation here
 */
public class StatisticsMap extends Hashtable<String, Double> {

    /**
     *
     */
    private static final long serialVersionUID = -2831335094438480701L;

    @SuppressWarnings(value = "rawtypes")
    public StatisticsMap(Hashtable<Class, Map<StatisticKeys, StatisticValue>> stats, Hashtable<Class<? extends Object>, Hashtable<String, CacheElement>> cache, long writeBufferEntries) {
        super.put(StatisticKeys.WRITE_BUFFER_ENTRIES.name(), (double) writeBufferEntries);

        double entries = 0;

        initWithStats(null, stats.get(Object.class));
        for (Class t : stats.keySet()) {
            if (t.equals(Object.class)) continue;

            initWithStats(t.getSimpleName(), stats.get(t));
        }
        super.put(StatisticKeys.CACHE_ENTRIES.name(), 0.0);
        for (Class k : cache.keySet()) {
            entries += cache.get(k).size();
            super.put("X-Entries for: " + k.getName(), (double) cache.get(k).size());
            super.put(StatisticKeys.CACHE_ENTRIES.name(), (double) super.get(StatisticKeys.CACHE_ENTRIES.name()) + cache.get(k).size());
        }

    }

    @Override
    public Set<String> keySet() {
        Set ks = super.keySet();
        ArrayList<String> l = new ArrayList<String>(ks);
        Collections.sort(l);
        return new LinkedHashSet<String>(l);
    }

    private void initWithStats(String prefix, Map<StatisticKeys, StatisticValue> stats) {
        if (prefix != null) {
            //Sorting it easier
            prefix = "XZ-Type: " + prefix + " / ";
        } else {
            prefix = "";
        }
        for (StatisticKeys k : stats.keySet()) {
            super.put(prefix + k.name(), (double) stats.get(k).get());
        }
        super.put(prefix + StatisticKeys.CHITSPERC.name(), ((double) stats.get(StatisticKeys.CHITS).get()) / (stats.get(StatisticKeys.READS).get() - stats.get(StatisticKeys.NO_CACHED_READS).get()) * 100.0);
        super.put(prefix + StatisticKeys.CMISSPERC.name(), ((double) stats.get(StatisticKeys.CMISS).get()) / (stats.get(StatisticKeys.READS).get() - stats.get(StatisticKeys.NO_CACHED_READS).get()) * 100.0);

    }

    @Override
    public synchronized Double put(String arg0, Double arg1) {
        throw new RuntimeException("not allowed!");
    }

    @Override
    public synchronized void putAll(@SuppressWarnings(value = "rawtypes") Map arg0) {
        throw new RuntimeException("not allowed");
    }

    @Override
    public synchronized Double remove(Object arg0) {
        throw new RuntimeException("not allowed");
    }

    @Override
    public String toString() {
        StringBuffer b = new StringBuffer();
        String[] lst = keySet().toArray(new String[]{});
        Arrays.sort(lst);
        for (String k : lst) {
            b.append("- ");
            b.append(k);
            b.append("\t");
            b.append(get(k));
            b.append("\n");
        }
        return b.toString();
    }
}