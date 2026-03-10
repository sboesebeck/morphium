package de.caluga.morphium;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.caching.Cache;

@Entity
@Cache
public class CollectionInfo {
    private String name;

    public String getName() {
        return name;
    }

    public CollectionInfo setName(String name) {
        this.name = name;
        return this;
    }
}
