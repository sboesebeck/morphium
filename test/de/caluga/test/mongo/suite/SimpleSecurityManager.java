package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.secure.MongoSecurityException;
import de.caluga.morphium.secure.MongoSecurityManager;
import de.caluga.morphium.secure.Permission;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 03.04.12
 * Time: 22:09
 * <p/>
 */
public class SimpleSecurityManager implements MongoSecurityManager {
    //TODO: implement some kind of login / user Session, maybe JAAS?

    private String user;
    private HashMap<String, List<Permission>> acl = new HashMap<String, List<Permission>>();

    public void login(String user) {
        this.user = user;
        acl.clear();
        List<Permission> lst = new ArrayList<Permission>();
        lst.add(Permission.READ);
        lst.add(Permission.INSERT);
        acl.put((Morphium.getConfig().getMapper().getCollectionName(UncachedObject.class)), lst);

    }


    @Override
    public boolean checkAccess(Object obj, Permission p) throws MongoSecurityException {
        if (obj == null) return false;
        return checkAccess(Morphium.getConfig().getMapper().getCollectionName(obj.getClass()), p);
    }

    @Override
    public boolean checkAccess(String domain, Permission p) throws MongoSecurityException {
        return acl.containsKey(domain) && acl.get(domain).contains(p);
    }

    @Override
    public boolean checkAccess(Class<?> cls, Permission p) throws MongoSecurityException {
        return checkAccess(Morphium.getConfig().getMapper().getCollectionName(cls), p);
    }

    @Override
    public Object getCurrentUserId() {
        return user;
    }
}
