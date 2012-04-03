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
    READ,    //access collection / domain
}
