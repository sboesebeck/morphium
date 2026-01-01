/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author stephan
 */
@NoCache
@Entity(typeId = "uc")
@WriteSafety(timeout = 10000, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
@DefaultReadPreference(ReadPreferenceLevel.NEAREST)
public class UncachedObject {
    @Index
    private String strValue;

    @Index
    private int counter;

    private double dval;

    private byte[] binaryData;
    private int[] intData;
    private long[] longData;
    private float[] floatData;
    private double[] doubleData;
    @IgnoreNullFromDB  // Test expects this field not to exist when null
    private boolean[] boolData;

    @Id
    private MorphiumId morphiumId;

    public UncachedObject() {

    }

    public UncachedObject(String strValue, int counter) {
        this.strValue = strValue;
        this.counter = counter;
    }

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

    public String getStrValue() {
        return strValue;
    }

    public void setStrValue(String strValue) {
        this.strValue = strValue;
    }


    public MorphiumId getMorphiumId() {
        return morphiumId;
    }

    public void setMorphiumId(MorphiumId morphiumId) {
        this.morphiumId = morphiumId;
    }

    @Override
    public String toString() {
        return "UncachedObject{" +
                "value='" + strValue + '\'' +
                ", counter=" + counter +
                ", dval=" + dval +
                ", binaryData=" + Arrays.toString(binaryData) +
                ", intData=" + Arrays.toString(intData) +
                ", longData=" + Arrays.toString(longData) +
                ", floatData=" + Arrays.toString(floatData) +
                ", doubleData=" + Arrays.toString(doubleData) +
                ", boolData=" + Arrays.toString(boolData) +
                ", morphiumId=" + morphiumId +
                '}';
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

        return counter == that.counter && Objects.equals(morphiumId, that.morphiumId) && Objects.equals(strValue, that.strValue);

    }

    @Override
    public int hashCode() {
        int result = strValue != null ? strValue.hashCode() : 0;
        result = 31 * result + counter;
        result = 31 * result + (morphiumId != null ? morphiumId.hashCode() : 0);
        return result;
    }


    public enum Fields {boolData, counter, doubleData, dval, floatData, intData, longData, morphiumId, strValue, binaryData}
}
