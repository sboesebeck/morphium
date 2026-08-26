package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.IgnoreNullFromDB;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;

import java.util.List;
import java.util.Map;

@Entity
public class InMemDumpContainer {
    @Id
    private Long created;
    private String db;
    private Map<String, List<Map<String, Object>>> data;
    /**
     * Index definitions per collection in MongoDB wire shape (the listIndexes/createIndexes
     * round-trip form), added for #340 - dumps used to lose every index across a full restart.
     * Strictly additive and OPTIONAL: {@code null} (the field is then absent from the dump JSON)
     * for dumps of databases without secondary indexes, and every reader - including all released
     * versions - ignores an absent or unknown key. The {@code _id} index is never included, it is
     * (re)seeded lazily by {@code InMemoryDriver.getIndexes()} anyway.
     */
    @IgnoreNullFromDB
    private Map<String, List<Map<String, Object>>> indexes;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public Long getCreated() {
        return created;
    }

    public void setCreated(Long created) {
        this.created = created;
    }

    public Map<String, List<Map<String, Object>>> getData() {
        return data;
    }

    public void setData(Map<String, List<Map<String, Object>>> data) {
        this.data = data;
    }

    public Map<String, List<Map<String, Object>>> getIndexes() {
        return indexes;
    }

    public void setIndexes(Map<String, List<Map<String, Object>>> indexes) {
        this.indexes = indexes;
    }
}
