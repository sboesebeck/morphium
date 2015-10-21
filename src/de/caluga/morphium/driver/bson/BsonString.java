package de.caluga.morphium.driver.bson;/**
 * Created by stephan on 24.09.15.
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * TODO: Add Documentation here
 **/
public class BsonString implements Bson {
    private Bson.Type type;
    private String value;

    public BsonString(String value) {
        type = Bson.Type.string;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public byte[] serialize() {
        ByteArrayOutputStream out = null;
        try {
            out = new ByteArrayOutputStream();
            out.write(value.getBytes("utf-8").length);
            out.write(value.getBytes("utf-8"));
            out.write(new byte[1]);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    @Override
    public Bson deserialize(byte[] in) {
        return null;
    }
}
