package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.AdditionalData;

import java.util.Map;

public class AdditionalDataEntity extends UncachedObject {
    @AdditionalData(readOnly = false)
    Map<String, Object> additionals;

    public Map<String, Object> getAdditionals() {
        return additionals;
    }

    public void setAdditionals(Map<String, Object> additionals) {
        this.additionals = additionals;
    }

    public enum Fields {binaryData, boolData, counter, doubleData, dval, floatData, intData, longData, morphiumId, value, additionals}
}
