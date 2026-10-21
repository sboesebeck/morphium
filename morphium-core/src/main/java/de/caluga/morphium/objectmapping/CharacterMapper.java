
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

        // Morphium writes the number of the character; another writer may have stored an int64 or a double, or the character as a string
        if (d instanceof Number number) {
            return Character.valueOf((char) number.intValue());
        }

        if (d instanceof String string && string.length() == 1) {
            return Character.valueOf(string.charAt(0));
        }

        throw new IllegalArgumentException("Cannot read a character from " + d.getClass().getName() + " " + d);
    }

}
