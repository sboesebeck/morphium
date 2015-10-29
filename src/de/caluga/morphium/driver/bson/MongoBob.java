package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 27.10.15.
 */

/**
 * TODO: Add Documentation here
 **/
public class MongoBob {
    private byte[] data;

    public MongoBob(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
