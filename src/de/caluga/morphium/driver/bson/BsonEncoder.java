package de.caluga.morphium.driver.bson;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 26.10.15
 * Time: 22:44
 * <p>
 * TODO: Add documentation here
 */
public class BsonEncoder {
    private HashMap<Class, Integer> typeMap;

    public BsonEncoder() {
        typeMap = new HashMap<>();
        typeMap.put(Double.class, 1);
        typeMap.put(double.class, 1);
        typeMap.put(String.class, 2);
        typeMap.put(BsonDoc.class, 3);
        typeMap.put(Map.class, 3);
        typeMap.put(List.class, 4);
        typeMap.put(Collection.class, 4);
        typeMap.put(byte[].class, 5);
        typeMap.put(Boolean.class, 8);
        typeMap.put(boolean.class, 8);
        typeMap.put(Date.class, 9);
        typeMap.put(java.sql.Date.class, 9);
        typeMap.put(Calendar.class, 9);
        typeMap.put(null, 10);
        typeMap.put(int.class, 16);
        typeMap.put(Integer.class, 16);
        typeMap.put(Long.class, 18);
        typeMap.put(long.class, 18);
    }


    public byte[] encodeString(String s) {
        return null;
    }
}
