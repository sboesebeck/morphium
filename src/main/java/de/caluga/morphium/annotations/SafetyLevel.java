package de.caluga.morphium.annotations;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 16:35
 * <p>
 * Define the type of Write safety.
 */
public enum SafetyLevel {
    NORMAL(0), @SuppressWarnings("unused")BASIC(1), WAIT_FOR_SLAVE(2), WAIT_FOR_ALL_SLAVES(3), MAJORITY(-99);

    private final int value;

    SafetyLevel(int v) {
        value = v;
    }

    public int getValue() {
        return value;
    }

}
