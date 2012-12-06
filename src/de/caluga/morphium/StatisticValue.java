package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 05.07.12
 * Time: 13:22
 * <p/>
 * Statistics
 */

public class StatisticValue {

    private long value = 0;

    public void inc() {
        value++;
    }

    public void dec() {
        value--;
    }

    public long get() {
        return value;
    }
}