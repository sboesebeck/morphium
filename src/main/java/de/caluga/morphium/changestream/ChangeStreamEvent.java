package de.caluga.morphium.changestream;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;

import java.util.List;
import java.util.Map;

@Entity(translateCamelCase = false)
public class ChangeStreamEvent {

    @Id
    private Object id;
    private String operationType;
    private Map<String, Object> fullDocument;
    private Map<String, Object> fullDocumentBeforeChange;
    private Map<String, String> ns;
    private Object documentKey;
    private Map<String, Object> updateDescription;
    private Map<String, Object> updatedFields;
    private List<String> removedFields;
    private long clusterTime;
    private long txnNumber;
    private String dbName;
    private String collectionName;
    private Map<String, Object> lsid;

    public List<String> getRemovedFields() {
        return removedFields;
    }

    public Map<String, Object> getUpdatedFields() {
        return updatedFields;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public Map<String, Object> getFullDocument() {
        return fullDocument;
    }

    public void setFullDocument(Map<String, Object> fullDocument) {
        this.fullDocument = fullDocument;
    }

    public Map<String, Object> getFullDocumentBeforeChange() {
        return fullDocumentBeforeChange;
    }

    public void setFullDocumentBeforeChange(Map<String, Object> fullDocumentBeforeChange) {
        this.fullDocumentBeforeChange = fullDocumentBeforeChange;
    }

    public Map<String, String> getNs() {
        return ns;
    }

    public void setNs(Map<String, String> ns) {
        this.ns = ns;
    }

    public Object getDocumentKey() {
        return documentKey;
    }

    public void setDocumentKey(Object documentKey) {
        this.documentKey = documentKey;
    }

    public Map<String, Object> getUpdateDescription() {
        return updateDescription;
    }

    public void setUpdateDescription(Map<String, Object> updateDescription) {
        this.updateDescription = updateDescription;
    }

    public long getClusterTime() {
        return clusterTime;
    }

    public void setClusterTime(long clusterTime) {
        this.clusterTime = clusterTime;
    }

    public long getTxnNumber() {
        return txnNumber;
    }

    public void setTxnNumber(long txnNumber) {
        this.txnNumber = txnNumber;
    }

    public Map<String, Object> getLsid() {
        return lsid;
    }

    public void setLsid(Map<String, Object> lsid) {
        this.lsid = lsid;
    }

    public <T> T getEntityFromData(Class<T> cls, Morphium m) {
        return m.getMapper().deserialize(cls, getFullDocument());
    }
}
