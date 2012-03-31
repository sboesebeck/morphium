/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.sync;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.StoreCreationTime;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.security.NoProtection;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Vector;

/**
 * @author stephan
 */
@NoCache
@NoProtection
@StoreCreationTime(creationTimeField = "created")
@Entity(collectionName = "db_sync")
public class DbSync {
    private long created;

    @Property(fieldName = "data_type")
    private String dataType;

    @Property(fieldName = "app_ids")
    private List<String> appIds;

    private String action;

    @Id
    private ObjectId mongoId;

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public List<String> getAppIds() {
        return appIds;
    }

    public void setAppIds(List<String> appIds) {
        this.appIds = appIds;
    }

    public void addAppId(String id) {
        if (appIds == null) {
            appIds = new Vector<String>();
        }
        appIds.add(id);
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public ObjectId getMongoId() {
        return mongoId;
    }

    public void setMongoId(ObjectId mongoId) {
        this.mongoId = mongoId;
    }
}
