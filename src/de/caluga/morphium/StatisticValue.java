package de.caluga.morphium;

/**
 * User: Stephan Bösebeck
 * Date: 31.03.12
 * Time: 18:30
 * <p/>
 * TODO: Add documentation here
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