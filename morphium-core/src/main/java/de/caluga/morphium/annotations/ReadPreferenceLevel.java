package de.caluga.morphium.annotations;


import de.caluga.morphium.driver.ReadPreference;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 13:54
 * <p>
 * Define the read preference level for this type. Read from PRimary only or allow to read from secondaries.
 */
@SuppressWarnings("UnusedDeclaration")
public enum ReadPreferenceLevel {
    PRIMARY(ReadPreference.primary()), PRIMARY_PREFERRED(ReadPreference.primaryPreferred()),
    SECONDARY(ReadPreference.secondary()), SECONDARY_PREFERRED(ReadPreference.secondaryPreferred()),
    NEAREST(ReadPreference.nearest());
    private ReadPreference pref;

    ReadPreferenceLevel(ReadPreference pref) {
        this.pref = pref;
    }

    public ReadPreference getPref() {
        return pref;
    }

    public void setPref(ReadPreference pref) {
        this.pref = pref;
    }
}
