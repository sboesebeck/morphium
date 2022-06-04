package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 27.10.15.
 */

/**
 * BOB implementation for BSON
 **/
@SuppressWarnings("WeakerAccess")
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
