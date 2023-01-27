package de.caluga.morphium.objectmapping;


public class ShortMapper implements MorphiumTypeMapper<Short> {

    @Override
    public Object marshall(Short o) {

        return o.intValue();
    }

    @Override
    public Short unmarshall(Object d) {
        if (d==null) return null;
        return Short.valueOf((short)(int)d);
    }

}
