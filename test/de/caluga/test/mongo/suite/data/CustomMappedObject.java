package de.caluga.test.mongo.suite.data;

public class CustomMappedObject {
    private String name;
    private String value;
    private int intValue;

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

    public int getIntValue() {
        return intValue;
    }

    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CustomMappedObject)) {
            return false;
        }

        CustomMappedObject that = (CustomMappedObject) obj;

        if (this.getName() == null && that.getName() != null) {
            return false;
        }
        if (!this.getName().equals(that.getName())) {
            return false;
        }
        if (this.getValue() == null && that.getValue() != null) {
            return false;
        }
        if (!this.getValue().equals(that.getValue())) {
            return false;
        }
        return this.getIntValue() == that.getIntValue();

    }
}
