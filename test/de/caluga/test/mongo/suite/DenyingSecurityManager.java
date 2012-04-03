package de.caluga.test.mongo.suite;

import de.caluga.morphium.secure.MongoSecurityException;
import de.caluga.morphium.secure.MongoSecurityManager;
import de.caluga.morphium.secure.Permission;

/**
 * User: Stephan Bösebeck
 * Date: 03.04.12
 * Time: 22:31
 * <p/>
 * TODO: Add documentation here
 */
public class DenyingSecurityManager implements MongoSecurityManager {
    @Override
    public boolean checkAccess(Object obj, Permission p) throws MongoSecurityException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean checkAccess(String domain, Permission p) throws MongoSecurityException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean checkAccess(Class<?> cls, Permission p) throws MongoSecurityException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object getCurrentUserId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
