package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;

import java.util.List;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 18:03
 * <p/>
 */
@NoCache
@Entity(translateCamelCase = false)
@LastAccess()
@CreationTime()
@LastChange()
@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
public class ComplexObject {
    @Id
    private MorphiumId id;

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

    private List<EmbeddedObject> embeddedObjectList;

    private String einText;

    @Transient
    private String trans;

    // Removed @UseIfNull - default behavior now accepts nulls
    private Integer nullValue;

    public List<EmbeddedObject> getEmbeddedObjectList() {
        return embeddedObjectList;
    }

    public void setEmbeddedObjectList(List<EmbeddedObject> embeddedObjectList) {
        this.embeddedObjectList = embeddedObjectList;
    }

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
        if (changed == null) return 0;
        return changed;
    }

    public void setChanged(long changed) {
        this.changed = changed;
    }

    public long getLastAccess() {
        if (lastAccess == null) return 0;
        return lastAccess;
    }

    public void setLastAccess(long lastAccess) {
        this.lastAccess = lastAccess;
    }

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
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
               ", ref=" + (ref != null ? ref.getMorphiumId().toString() : "null") +
               ", entityEmbedded=" + entityEmbeded +
               ", einText='" + einText + '\'' +
               ", trans='" + trans + '\'' +
               ", nullValue=" + nullValue +
               '}';
    }


    public enum Fields {created, cRef, einText, embed, embeddedObjectList, entityEmbeded, id, lastAccess, nullValue, ref, trans, changed}
}
