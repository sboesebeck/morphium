package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Reference;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 15:03
 * <p/>
 */
@NoCache
@Entity
public class ComplexObject {
    @Id
    private ObjectId id;

    @Reference(fieldName = "reference")
    private UncachedObject ref;

    private UncachedObject embed;

    private String einText;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public UncachedObject getRef() {
        return ref;
    }

    public void setRef(UncachedObject ref) {
        this.ref = ref;
    }

    public UncachedObject getEmbed() {
        return embed;
    }

    public void setEmbed(UncachedObject embed) {
        this.embed = embed;
    }

    public String getEinText() {
        return einText;
    }

    public void setEinText(String einText) {
        this.einText = einText;
    }

    @Override
    public String toString() {
        return "ComplexObject{" +
                "id=" + id +
                ", ref=" + (ref != null ? ref.getMongoId() : "null") +
                ", embed={" + embed +
                "}, einText='" + einText + '\'' +
                '}';
    }
}
