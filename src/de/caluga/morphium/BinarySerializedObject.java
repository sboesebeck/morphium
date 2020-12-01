package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;

/**
 * Created by stephan on 17.11.14.
 */
@SuppressWarnings({"WeakerAccess", "DefaultFileTemplate"})
@Embedded
public class BinarySerializedObject {
    @Property(fieldName = "_b64data")
    private String b64Data;
    private String originalClassName;


    public String getB64Data() {
        return b64Data;
    }

    public void setB64Data(String b64Data) {
        this.b64Data = b64Data;
    }
//  no one can access this object directly, it is only used internally for serialization
    // de-serialization does not create this object
//    @SuppressWarnings("unused")
//    public String getOriginalClassName() {
//        return originalClassName;
//    }

    public void setOriginalClassName(String originalClassName) {
        this.originalClassName = originalClassName;
    }
}
