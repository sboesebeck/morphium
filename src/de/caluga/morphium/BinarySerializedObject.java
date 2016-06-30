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

    @SuppressWarnings("unused")
    public String getOriginalClassName() {
        return originalClassName;
    }

    public void setOriginalClassName(String originalClassName) {
        this.originalClassName = originalClassName;
    }
}
