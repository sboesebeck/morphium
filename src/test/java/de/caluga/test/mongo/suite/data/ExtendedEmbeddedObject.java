package de.caluga.test.mongo.suite.data;

/**
 * @author martinf (07.07.16)
 */
public class ExtendedEmbeddedObject extends EmbeddedObject {

    private String additionalValue;

    public String getAdditionalValue() {
        return additionalValue;
    }

    public void setAdditionalValue(String additionalValue) {
        this.additionalValue = additionalValue;
    }
}
