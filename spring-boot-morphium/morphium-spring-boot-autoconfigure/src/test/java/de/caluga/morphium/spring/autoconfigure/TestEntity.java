package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;

@Entity
public class TestEntity {
    @Id
    private MorphiumId id;
    private String name;
    private String status;
    private int priority;

    public TestEntity() {}

    public TestEntity(String name, String status, int priority) {
        this.name = name;
        this.status = status;
        this.priority = priority;
    }

    public MorphiumId getId() { return id; }
    public void setId(MorphiumId id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }
}
