package de.caluga.morphium.mapping;/**
 * Created by stephan on 18.09.15.
 */


import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * custom type mapper for BigIntegers
 **/
public class BigIntegerTypeMapper {
    public Object marshall(BigInteger o) {
        //        DBObject ret=new BasicDBObject();
        Map<String, Object> obj = new HashMap<>();
        obj.put("type", "biginteger");
        obj.put("value", o.toString(16));
        return obj;
    }

    public BigInteger unmarshall(Object d) {
        if (d == null) {
            return null;
        }
        if (d instanceof Map) {
            return new BigInteger(((Map) d).get("value").toString(), 16);
        }
        return new BigInteger(d.toString(), 16);
    }

    public boolean matches(Object v) {
        if (v instanceof Map) {
            Map value = (Map) v;
            return value != null && value.containsKey("type") && value.get("type").equals("biginteger") && value.get("value") instanceof String;
        }
        return false;
    }
}
