/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable;

import com.mongodb.DBObject;

import java.util.Map;

/**
 * @author stephan
 */
public interface RecordTableSearchableFieldListener {
    public boolean valueUpdate(String currentValue, Map<String, DBObject> currentSearch);
}
