
package de.caluga.morphium.objectmapping;

public class CharacterMapper implements MorphiumTypeMapper<Character> {

    @Override
    public Object marshall(Character o) {
        return (int) o.charValue();
    }

    @Override
    public Character unmarshall(Object d) {
        if (d == null) {
            return null;
        }

        return Character.valueOf((char)((int) d));
    }

}
