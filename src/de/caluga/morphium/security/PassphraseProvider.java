package de.caluga.morphium.security;

public interface PassphraseProvider {
    public byte[] getPassphraseFor(Class type, String field);
}
