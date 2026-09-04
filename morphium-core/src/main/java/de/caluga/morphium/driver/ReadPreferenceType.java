package de.caluga.morphium.driver;

/**
 * Created by stephan on 05.11.15.
 */
@SuppressWarnings("DefaultFileTemplate")
public enum ReadPreferenceType {
    PRIMARY("primary"),
    SECONDARY("secondary"),
    PRIMARY_PREFERRED("primaryPreferred"),
    SECONDARY_PREFERRED("secondaryPreferred"),
    NEAREST("nearest");

    private final String mode;

    ReadPreferenceType(String mode) {
        this.mode = mode;
    }

    /**
     * @return the name this read preference has in the wire protocol's {@code $readPreference.mode}
     */
    public String getMode() {
        return mode;
    }
}
