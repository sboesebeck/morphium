package de.caluga.morphium.objectmapping;

public class ByteMapper implements MorphiumTypeMapper<Byte>{

    @Override
    public Object marshall(Byte o) {
        return o.byteValue();
    }

    @Override
    public Byte unmarshall(Object d) {
        if (d==null) return null;
        return new Byte((byte)d);
    }



}
