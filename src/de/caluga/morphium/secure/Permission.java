/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.secure;

/**
 * @author stephan
 */
public enum Permission {
    UPDATE,  //usually: update Object
    INSERT,  //Add a new record
    DELETE,  //delete Object
    DROP,    //Drop Collection
    FIND,    //readAll with template
    LISTALL, //readAll all elements of type
    COUNT,   //ElementCount for search
    COUNTALL, //Get an elementcount for whole collection
    DISABLE_READ_CACHE, //Disable read for type
    ENABLE_READ_CACHE,  //enable cache for type
    //addidtional Permissions, maybe used by application
    SHOW, EXECUTE, SEND,
}
