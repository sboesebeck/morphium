/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.secure;

/**
 * @author stephan
 */
public interface MongoSecurityManager {
    /**
     * Called by layer when access to a given type occurs.
     * Obj may be the record to modify or the template being searched for.
     * This should be implemented by the application in order to be able to
     * implement security settings depending on authenticated user.
     * <p/>
     * Morphium supports the following permissions:
     * INSERT: inserts a new record into db
     * UPDATE: update specific record
     * DELETE: delete record or collection
     * FIND: search for specified records (may alter template though!)
     * LISTALL: readAll all elements of type. Object then is an empty one
     * Additional Permissions (maybe used by Application):
     * EXECUTE, SHOW,
     *
     * @param obj
     * @param p
     * @return true if access is granted, false otherwise - can throw SecurityException with additional Information if necessary.
     */
    public boolean checkAccess(Object obj, Permission p) throws MongoSecurityException;

    /**
     * more flexible access for security. Domain may be any string, or classname
     *
     * @param domain
     * @param p
     * @return
     */
    public boolean checkAccess(String domain, Permission p) throws MongoSecurityException;


    /**
     * depending on implementation, this might be an ObjectId or a login string or whatever
     * ATTENTION: needs to be compatible with @StoreCreationTime and @StoreLAstAccess etc.
     * @return
     */
    public Object getCurrentUserId();


}
