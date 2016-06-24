/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.bson.MorphiumId;

/**
 * @author stephan
 */
@NoCache
@Entity
@WriteSafety(waitForSync = true, timeout = -1, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
@DefaultReadPreference(ReadPreferenceLevel.NEAREST)
public class UncachedObject {
    @Index
    private String value;

    @Index
    private int counter;

    private double dval;

    private byte[] binaryData;
    private int[] intData;
    private long[] longData;
    private float[] floatData;
    private double[] doubleData;
    private boolean[] boolData;

    @Id
    private MorphiumId morphiumId;

    public double getDval() {
        return dval;
    }

    public void setDval(double dval) {
        this.dval = dval;
    }

    public double[] getDoubleData() {
        return doubleData;
    }

    public void setDoubleData(double[] doubleData) {
        this.doubleData = doubleData;
    }

    public int[] getIntData() {
        return intData;
    }

    public void setIntData(int[] intData) {
        this.intData = intData;
    }

    public long[] getLongData() {
        return longData;
    }

    public void setLongData(long[] longData) {
        this.longData = longData;
    }

    public float[] getFloatData() {
        return floatData;
    }

    public void setFloatData(float[] floatData) {
        this.floatData = floatData;
    }

    public boolean[] getBoolData() {
        return boolData;
    }

    public void setBoolData(boolean[] boolData) {
        this.boolData = boolData;
    }

    public byte[] getBinaryData() {
        return binaryData;
    }

    public void setBinaryData(byte[] binaryData) {
        this.binaryData = binaryData;
    }

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


    public MorphiumId getMorphiumId() {
        return morphiumId;
    }

    public void setMorphiumId(MorphiumId morphiumId) {
        this.morphiumId = morphiumId;
    }

    public String toString() {
        return "Counter: " + counter + " Value: " + value + " MongoId: " + morphiumId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UncachedObject that = (UncachedObject) o;

        return counter == that.counter && !(morphiumId != null ? !morphiumId.equals(that.morphiumId) : that.morphiumId != null) && !(value != null ? !value.equals(that.value) : that.value != null);

    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + counter;
        result = 31 * result + (morphiumId != null ? morphiumId.hashCode() : 0);
        return result;
    }


    public enum Fields {counter, binaryData, intData, longData, floatData, doubleData, boolData, mongoId, value}
}
