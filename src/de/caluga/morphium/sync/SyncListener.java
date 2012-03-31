/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.sync;

/**
 * @author stephan
 */
public interface SyncListener {
    public void syncEvent(Class<? extends Object> type, String action);
}
