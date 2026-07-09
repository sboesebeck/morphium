package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 27.10.15.
 */

/**
 * BOB implementation for BSON
 *
 * @deprecated wrap raw BSON bytes in a plain {@code byte[]} (encoded as BSON binary)
 * instead; will be removed in 7.0 together with its {@code BsonEncoder} support.
 **/
@SuppressWarnings("WeakerAccess")
@Deprecated(since = "6.3", forRemoval = true)
public class MongoBob {
    private byte[] data;

    public MongoBob(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    @SuppressWarnings("unused")
    public void setData(byte[] data) {
        this.data = data;
    }
}
