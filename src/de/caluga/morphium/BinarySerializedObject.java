package de.caluga.morphium;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import org.bson.types.ObjectId;

/**
 * Created by stephan on 17.11.14.
 */
@Embedded
public class BinarySerializedObject {
    private String b64Data;
    private String originalClassName;


    public String getB64Data() {
        return b64Data;
    }

    public void setB64Data(String b64Data) {
        this.b64Data = b64Data;
    }

    public String getOriginalClassName() {
        return originalClassName;
    }

    public void setOriginalClassName(String originalClassName) {
        this.originalClassName = originalClassName;
    }
}
