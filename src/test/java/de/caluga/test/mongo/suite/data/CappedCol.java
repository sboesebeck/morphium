package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Capped;

@Capped(maxEntries = 10, maxSize = 100000)
public class CappedCol extends UncachedObject {
    public CappedCol() {
    }

    public CappedCol(String value, int counter) {
        super(value, counter);
    }

    public enum Fields {boolData, counter, doubleData, dval, floatData, intData, longData, morphiumId, value, binaryData}
}
