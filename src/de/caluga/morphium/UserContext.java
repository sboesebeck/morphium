/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium;

/**
 * @author stephan
 */
public interface UserContext {
    public String getCurrentUserId();

    public Object getCurrentUser();

    public boolean isUserInRole(Object role);
}
