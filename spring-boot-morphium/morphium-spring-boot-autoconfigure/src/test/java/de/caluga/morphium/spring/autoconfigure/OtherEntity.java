package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;

@Entity
public class OtherEntity {
    @Id
    private MorphiumId id;
    private String label;
    private int size;

    public OtherEntity() {}

    public OtherEntity(String label, int size) {
        this.label = label;
        this.size = size;
    }

    public MorphiumId getId() { return id; }
    public String getLabel() { return label; }
    public int getSize() { return size; }
}
