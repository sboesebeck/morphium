package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;

import java.util.List;
import java.util.Map;

@Entity
public class InMemDumpContainer {
    @Id
    private Long created;
    private String db;
    private Map<String, List<Doc>> data;

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

    public Map<String, List<Doc>> getData() {
        return data;
    }

    public void setData(Map<String, List<Doc>> data) {
        this.data = data;
    }
}
