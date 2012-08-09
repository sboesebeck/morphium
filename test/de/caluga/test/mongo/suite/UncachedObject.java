/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

/**
 * @author stephan
 */
@NoCache
@Entity
@WriteSafety(waitForJournalCommit = false, waitForSync = false, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
public class UncachedObject {
    @Index
    private String value;

    @Index
    private int counter;


    private
    @Id
    ObjectId mongoId;

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    @PartialUpdate("value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public ObjectId getMongoId() {
        return mongoId;
    }

    public void setMongoId(ObjectId mongoId) {
        this.mongoId = mongoId;
    }

    public String toString() {
        return "Counter: " + counter + " Value: " + value + " MongoId: " + mongoId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UncachedObject that = (UncachedObject) o;

        if (counter != that.counter) return false;
        if (mongoId != null ? !mongoId.equals(that.mongoId) : that.mongoId != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + counter;
        result = 31 * result + (mongoId != null ? mongoId.hashCode() : 0);
        return result;
    }
}
