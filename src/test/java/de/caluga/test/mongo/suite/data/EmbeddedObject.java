package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Embedded;

import java.util.Objects;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 16:54
 * <p>
 */
@Embedded(typeId = "embedded")
public class EmbeddedObject {
    private String name;
    private String value;
    private long testValueLong;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTest() {
        return testValueLong;
    }

    public void setTest(long test) {
        this.testValueLong = test;
    }

    public EmbeddedObject() {
    }

    public EmbeddedObject(String name, String value, long testValueLong) {
        this.name = name;
        this.value = value;
        this.testValueLong = testValueLong;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EmbeddedObject)) {
            return false;
        }

        EmbeddedObject that = (EmbeddedObject) o;

        return testValueLong == that.testValueLong && Objects.equals(name, that.name) && Objects.equals(value, that.value);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (testValueLong ^ (testValueLong >>> 32));
        return result;
    }

    public enum Fields {testValueLong, value, name}
}
