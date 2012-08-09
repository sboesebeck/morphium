package de.caluga.morphium.annotations;

/**
 * User: Stephan Bösebeck
 * Date: 26.05.12
 * Time: 16:35
 * <p/>
 * TODO: Add documentation here
 */
public enum SafetyLevel {
    IGNORE_ERROR(-1), NORMAL(0), BASIC(1), WAIT_FOR_SLAVE(2), WAIT_FOR_ALL_SLAVES(3);

    int value;

    SafetyLevel(int v) {
        value = v;
    }

    public int getValue() {
        return value;
    }

}
