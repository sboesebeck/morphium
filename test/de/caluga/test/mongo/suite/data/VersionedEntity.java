package de.caluga.test.mongo.suite.data;


import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Version;

@Entity(autoVersioning = true)
public class VersionedEntity extends UncachedObject {

    @Version
    private long theVersionNumber;

    public VersionedEntity() {
        super();
    }

    public VersionedEntity(String value, int counter) {
        super(value, counter);
    }

    public long getTheVersionNumber() {
        return theVersionNumber;
    }

    public void setTheVersionNumber(long theVersionNumber) {
        this.theVersionNumber = theVersionNumber;
    }

    public enum Fields {boolData, counter, doubleData, dval, floatData, intData, longData, morphiumId, theVersionNumber, strValue, binaryData}
}
