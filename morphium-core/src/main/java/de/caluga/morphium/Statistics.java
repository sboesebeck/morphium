package de.caluga.morphium;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("StringBufferMayBeStringBuilder")
public class Statistics extends HashMap<String, Double> {
    private static final long serialVersionUID = -2368547656520608318L;
    private final transient Morphium morphium;


    @SuppressWarnings("rawtypes")
    public Statistics(Morphium morphium) {
        this.morphium = morphium;

        for (Map.Entry<StatisticKeys, StatisticValue> et : morphium.getStats().entrySet()) {
            super.put(et.getKey().name(), (double) et.getValue().get());
        }

        double entries = 0;

        if (morphium.getCache() != null) {
            Map<String, Integer> map = morphium.getCache().getSizes();

            for (Map.Entry<String, Integer> e : map.entrySet()) {
                super.put("X-Entries for: " + e.getKey(), (double) e.getValue());
                entries += e.getValue();
            }

            super.put(StatisticKeys.CACHE_ENTRIES.name(), entries);

            long chits = morphium.getStats().get(StatisticKeys.CHITS).get();
            long cmiss = morphium.getStats().get(StatisticKeys.CMISS).get();
            long total = chits + cmiss;
            // Before any cached read has happened, chits+cmiss is 0 -- report
            // 0% rather than 0.0/0.0 = NaN (NaN samples are silently dropped
            // by Prometheus/OTel exporters, so consumers saw the metric
            // simply missing instead of a real "no data yet" zero).
            super.put(StatisticKeys.CHITSPERC.name(), total == 0 ? 0.0 : ((double) chits) / total * 100.0);
            super.put(StatisticKeys.CMISSPERC.name(), total == 0 ? 0.0 : ((double) cmiss) / total * 100.0);
        }

        super.put(StatisticKeys.WRITE_BUFFER_ENTRIES.name(), (double) morphium.getWriteBufferCount());
    }


    @SuppressWarnings("unused")
    public Double get(Enum key) {
        return get(key.name());
    }

    @Override
    public Double put(String arg0, Double arg1) {
        throw new RuntimeException("not allowed!");
    }

    @Override
    public void putAll(@SuppressWarnings("rawtypes") Map arg0) {
        throw new RuntimeException("not allowed");
    }

    @Override
    public Double remove(Object arg0) {
        throw new RuntimeException("not allowed");
    }

    @SuppressWarnings("EmptyMethod")
    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @SuppressWarnings("EmptyMethod")
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        StatisticKeys[] lst = morphium.getStats().keySet().toArray(new StatisticKeys[0]);
        Arrays.sort(lst);

        for (StatisticKeys k : lst) {
            b.append("- ");
            b.append(k.toString());
            b.append("\t");
            b.append(morphium.getStats().get(k));
            b.append("\n");
        }

        return b.toString();
    }
}
