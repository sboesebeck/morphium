
package de.caluga.morphium.objectmapping;

public class CharacterMapper implements MorphiumTypeMapper<Character> {

    @Override
    public Object marshall(Character o) {
        return (int) o.charValue();
    }

    @Override
    public Character unmarshall(Object d) {
        return d == null ? null : read(d);
    }

    /**
     * The character that a stored value stands for. Morphium writes the number of the character; another writer may have stored an int64 or a double, or the character
     * as a string of length one. A number that is no character ({@code NaN}, below 0, above 65535) is refused instead of wrapping around to another character.
     *
     * @throws IllegalArgumentException if the value is none of these
     */
    public static Character read(Object d) {
        if (d instanceof Number number) {
            double value = number.doubleValue();

            if (Double.isNaN(value) || value < Character.MIN_VALUE || value > Character.MAX_VALUE) {
                throw new IllegalArgumentException("The number " + d + " is not a character (0 to 65535)");
            }

            return Character.valueOf((char) number.intValue());
        }

        if (d instanceof String string && string.length() == 1) {
            return Character.valueOf(string.charAt(0));
        }

        throw new IllegalArgumentException("Cannot read a character from " + d.getClass().getName() + " " + d);
    }

}
