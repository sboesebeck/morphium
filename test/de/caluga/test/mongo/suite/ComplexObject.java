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
@Entity(translateCamelCase = false)
@StoreLastAccess()
@StoreCreationTime()
@StoreLastChange()
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
public class ComplexObject {
    @Id
    private ObjectId id;

    @Aliases({"last_changed", "lastChanged"})
    @LastChange
    private Long changed;
    @CreationTime
    private Long created;

    @Property(fieldName = "last_access")
    @LastAccess
    private Long lastAccess;

    @Reference(fieldName = "reference")
    private UncachedObject ref;

    @Reference(fieldName = "cached_reference")
    private CachedObject cRef;

    private EmbeddedObject embed;

    private UncachedObject entityEmbeded;

    @NotNull
    private String einText;

    @Transient
    private String trans;

    @UseIfnull
    private Integer nullValue;

    public CachedObject getcRef() {
        return cRef;
    }

    public void setcRef(CachedObject cRef) {
        this.cRef = cRef;
    }

    public Long getCreated() {
        return created;
    }

    public void setCreated(Long created) {
        this.created = created;
    }

    public UncachedObject getEntityEmbeded() {
        return entityEmbeded;
    }

    public void setEntityEmbeded(UncachedObject entityEmbeded) {
        this.entityEmbeded = entityEmbeded;
    }

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

    public EmbeddedObject getEmbed() {
        return embed;
    }

    public void setEmbed(EmbeddedObject embed) {
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
                ", ref=" + (ref != null ? ref.getMongoId().toString() : "null") +
                ", entityEmbedded=" + entityEmbeded +
                ", einText='" + einText + '\'' +
                ", trans='" + trans + '\'' +
                ", nullValue=" + nullValue +
                '}';
    }
}
