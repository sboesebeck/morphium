package de.caluga.morphium.security;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Property;

import java.lang.reflect.Field;

public class DefaultPassphraseProvider implements PassphraseProvider {
    @Override
    public byte[] getPassphraseFor(Class type, String field) {

        try {
            Field f=type.getDeclaredField(field);
            Property p=f.getAnnotation(Property.class);
            if (p!=null && !p.passphrase().equals(".")){
                return p.passphrase().getBytes();
            }
        } catch (NoSuchFieldException swallow) {
            //swallow
        }

        Entity e = (Entity) type.getAnnotation(Entity.class);
        if (e != null && !e.passphrase().equals(".")) {
            return e.passphrase().getBytes();
        }
        Embedded emb = (Embedded) type.getAnnotation(Embedded.class);
        if (emb != null && !emb.passphrase().equals(".")) {
            return e.passphrase().getBytes();

        }
        return null;
    }
}
