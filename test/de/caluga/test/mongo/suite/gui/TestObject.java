/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite.gui;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.PanelClass;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

/**
 * @author stephan
 */
@NoCache
@Entity
@PanelClass(TestObjectPanel.class)
public class TestObject {

    private
    @Id
    ObjectId id;

    private
    @Property
    String data;

    private
    @Property
    String test;

    private
    @Property
    String bla;

    private
    @Property
    int counter;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public String getBla() {
        return bla;
    }

    public void setBla(String bla) {
        this.bla = bla;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getTest() {
        return test;
    }

    public void setTest(String test) {
        this.test = test;
    }


}
