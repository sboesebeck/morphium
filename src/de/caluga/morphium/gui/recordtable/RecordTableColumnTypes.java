/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable;

/**
 * @author stephan
 */
public enum RecordTableColumnTypes {
    //no Check
    TEXT,
    //no check, mut make sure it's a int
    INTEGER,
    //no check, just make sure it's a float
    FLOAT,
    //currency check
    CURRENCY,
    //Date / Time fields
    DATE, DATETIME, TIME
}
