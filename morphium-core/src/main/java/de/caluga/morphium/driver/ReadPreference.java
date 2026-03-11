package de.caluga.morphium.driver;/**
 * Created by stephan on 05.11.15.
 */

import java.util.HashMap;
import java.util.Map;

/**
 * Read preference defines which node will be used for processing a query.
 * also tagsets can be set here.
 **/
public class ReadPreference {
    private ReadPreferenceType type;
    private Map<String, String> tagSet;

    public static ReadPreference primary() {
        ReadPreference rp = new ReadPreference();
        rp.setType(ReadPreferenceType.PRIMARY);
        return rp;
    }

    public static ReadPreference primaryPreferred() {
        ReadPreference rp = new ReadPreference();
        rp.setType(ReadPreferenceType.PRIMARY_PREFERRED);
        return rp;
    }

    public static ReadPreference secondary() {
        ReadPreference rp = new ReadPreference();
        rp.setType(ReadPreferenceType.SECONDARY);
        return rp;
    }

    public static ReadPreference secondaryPreferred() {
        ReadPreference rp = new ReadPreference();
        rp.setType(ReadPreferenceType.SECONDARY_PREFERRED);
        return rp;

    }

    public static ReadPreference nearest() {
        ReadPreference rp = new ReadPreference();
        rp.setType(ReadPreferenceType.NEAREST);
        return rp;
    }

    public ReadPreferenceType getType() {
        return type;
    }

    public void setType(ReadPreferenceType type) {
        this.type = type;
    }

    public Map<String, String> getTagSet() {
        return tagSet;
    }

    @SuppressWarnings("unused")
    public void setTagSet(Map<String, String> tagSet) {
        this.tagSet = tagSet;
    }

    @SuppressWarnings("unused")
    public void addTag(String key, String value) {
        if (tagSet == null) {
            tagSet = new HashMap<>();
        }
        tagSet.put(key, value);
    }
}
