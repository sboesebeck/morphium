package de.caluga.morphium.driver.commands;

import java.util.List;
import java.util.Map;

public class StoreMongoCommand extends WriteMongoCommand<StoreMongoCommand> {
    private List<Map<String, Object>> docs;

    public List<Map<String, Object>> getDocs() {
        return docs;
    }

    public StoreMongoCommand setDocs(List<Map<String, Object>> docs) {
        this.docs = docs;
        return this;
    }
}
