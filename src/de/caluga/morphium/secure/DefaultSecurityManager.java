/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.secure;

/**
 * @author stephan
 */
public class DefaultSecurityManager implements MongoSecurityManager {

    @Override
    public boolean checkAccess(Object obj, Permission p) throws MongoSecurityException {
        return true;
    }

    @Override
    public boolean checkAccess(String domain, Permission p) throws MongoSecurityException {
        return true;
    }

}
