package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 18:03
 * <p/>
 */
@NoCache
@Entity
@StoreLastAccess(lastAccessField = "last_access")

//cration time will be stored in DB but not in Object!
@StoreCreationTime(creationTimeField = "created")
@StoreLastChange(lastChangeField = "changed")
public class ComplexObject {
    @Id
    private ObjectId id;

    @Aliases({"last_changed","lastChanged"})
    private Long changed;

    @Property(fieldName = "last_access")
    private Long lastAccess;

    @Reference(fieldName = "reference")
    private UncachedObject ref;

    private UncachedObject embed;

    @NotNull
    private String einText;

    @Transient
    private String trans;

    @UseIfnull
    private Integer nullValue;

    public long getChanged() {
        return changed;
    }

    public void setChanged(long changed) {
        this.changed = changed;
    }

    public long getLastAccess() {
        return lastAccess;
    }

    public void setLastAccess(long lastAccess) {
        this.lastAccess = lastAccess;
    }

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

    public String getTrans() {
        return trans;
    }

    public void setTrans(String trans) {
        this.trans = trans;
    }

    public Integer getNullValue() {
        return nullValue;
    }

    public void setNullValue(Integer nullValue) {
        this.nullValue = nullValue;
    }

    @Override
    public String toString() {
        return "ComplexObject{" +
                "id=" + id +
                ", ref=" + (ref!=null?ref.getMongoId().toString():"null") +
                ", embed=" + embed +
                ", einText='" + einText + '\'' +
                ", trans='" + trans + '\'' +
                ", nullValue=" + nullValue +
                '}';
    }
}
