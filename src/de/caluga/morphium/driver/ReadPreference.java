package de.caluga.morphium.driver;/**
 * Created by stephan on 05.11.15.
 */

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class ReadPreference {
    private ReadPreferenceType type;
    private Map<String, String> tagSet;

    public ReadPreferenceType getType() {
        return type;
    }

    public void setType(ReadPreferenceType type) {
        this.type = type;
    }

    public Map<String, String> getTagSet() {
        return tagSet;
    }

    public void setTagSet(Map<String, String> tagSet) {
        this.tagSet = tagSet;
    }

    public void addTag(String key, String value) {
        if (tagSet == null) tagSet = new HashMap<>();
        tagSet.put(key, value);
    }
}
