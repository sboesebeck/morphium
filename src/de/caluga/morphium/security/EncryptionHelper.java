package de.caluga.morphium.security;

import de.caluga.rsa.AES;
import de.caluga.rsa.RSA;

public class EncryptionHelper {

    public static byte[] encrypt(Encryption method, byte[] passphrase, String toEncrypt) {
        switch (method) {
            case AES256:

                byte[] p = fixPassphraseLength(passphrase);
                AES a = new AES();
                a.setEncryptionKey(p);
                return a.encrypt(toEncrypt);
            case RSA:
                //RSA r=new RSA();

                throw new RuntimeException("RSA encryption not implemented yet, sorry!");
            default:
                throw new RuntimeException("Unknown encryption method " + method.name());
        }
    }

    public static byte[] decrypt(Encryption method, byte[] passphrase, byte[] toDecrypt){
        switch (method){
            case AES256:
                byte[] p = fixPassphraseLength(passphrase);
                AES a = new AES();
                a.setEncryptionKey(p);
                return a.decrypt(toDecrypt);
            default:
                throw new RuntimeException("Unknown encryption method " + method.name());
        }
    }

    private static byte[] fixPassphraseLength(byte[] passphrase) {
        int l=passphrase.length;
        byte[] p;
        if (l!=16 && l!=24 && l!=32){
            if (l>32){
                l=32;
            } else if (l>24){
                l=32;
            } else if (l>16){
                l=24;
            } else {
                l=16;
            }
            p=new byte[l];
            for (int i=0;i<l;i++) p[i]=0;
            System.arraycopy(passphrase,0,p,0,passphrase.length>l?l:passphrase.length);
        } else {
            p=passphrase;
        }
        return p;
    }
}
