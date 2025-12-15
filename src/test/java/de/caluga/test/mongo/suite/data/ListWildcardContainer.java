package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Martin Finke
 * Date: 07.07.16
 * Time: 08:31
 * <p>
 */
@Entity
public class ListWildcardContainer {
    @Id
    MorphiumId id;

    private String name;

    private List<? extends EmbeddedObject> embeddedObjectList;

    public ListWildcardContainer() {
        embeddedObjectList = new ArrayList<>();
    }

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<? extends EmbeddedObject> getEmbeddedObjectList() {
        return embeddedObjectList;
    }

    public void setEmbeddedObjectList(List<? extends EmbeddedObject> embeddedObjectList) {
        this.embeddedObjectList = embeddedObjectList;
    }

    public enum Fields {id, name, embeddedObjectList}
}
