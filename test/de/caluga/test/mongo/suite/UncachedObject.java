/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

/**
 * @author stephan
 */
@NoCache
@Entity
public class UncachedObject {
    private String value;

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
}
