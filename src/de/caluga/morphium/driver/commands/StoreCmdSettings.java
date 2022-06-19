package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.List;
import java.util.Map;

public class StoreCmdSettings extends WriteCmdSettings<StoreCmdSettings> {
    private List<Map<String, Object>> docs;

    public List<Map<String, Object>> getDocs() {
        return docs;
    }

    public StoreCmdSettings setDocs(List<Map<String, Object>> docs) {
        this.docs = docs;
        return this;
    }
}
