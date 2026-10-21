package de.caluga.morphium.objectmapping;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.Map;

/**
 * A {@code short} or {@code Short} field is a number in the database, like an int (MOR-084). Without this mapper it was written as a Base64 Java-serialization blob
 * ({@code {_b64data: ...}}), which cannot be compared, sorted or incremented by the database. Documents that were written that way are still read: the blob is decoded, and
 * only a {@code Short} may come out of it.
 */
public class ShortMapper implements MorphiumTypeMapper<Short> {

    private static final ObjectInputFilter ONLY_SHORT = ObjectInputFilter.Config.createFilter("java.lang.Short;java.lang.Number;!*");

    @Override
    public Object marshall(Short o) {
        return o.intValue();
    }

    @Override
    public Short unmarshall(Object d) {
        if (d == null) {
            return null;
        }

        if (d instanceof Number n) {
            return n.shortValue();
        }

        if (d instanceof Map<?, ?> m) {
            Object blob = m.get("_b64data") != null ? m.get("_b64data") : m.get("b64Data");

            if (blob instanceof String s) {
                try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(Base64.getMimeDecoder().decode(s)))) {
                    in.setObjectInputFilter(ONLY_SHORT);

                    if (in.readObject() instanceof Short sh) {
                        return sh;
                    }
                } catch (IOException | ClassNotFoundException e) {
                    throw new IllegalArgumentException("A short cannot be read from the stored value: " + e.getMessage(), e);
                }
            }
        }

        throw new IllegalArgumentException("A short cannot be read from " + d.getClass().getName());
    }
}
