package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Embedded;

/**
 * User: Stephan Bösebeck
 * Date: 28.05.12
 * Time: 16:54
 * <p/>
 */
@Embedded
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EmbeddedObject)) {
            return false;
        }

        EmbeddedObject that = (EmbeddedObject) o;

        return testValueLong == that.testValueLong && !(name != null ? !name.equals(that.name) : that.name != null) && !(value != null ? !value.equals(that.value) : that.value != null);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (testValueLong ^ (testValueLong >>> 32));
        return result;
    }
}
