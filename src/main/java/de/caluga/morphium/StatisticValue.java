package de.caluga.morphium;

import java.util.concurrent.atomic.AtomicLong;

/**
 * User: Stephan Bösebeck
 * Date: 05.07.12
 * Time: 13:22
 * <p>
 * Statistics
 */

@SuppressWarnings("UnusedDeclaration")
public class StatisticValue extends AtomicLong {

    public void inc() {
        incrementAndGet();
    }


    public void dec() {
        decrementAndGet();
    }


}