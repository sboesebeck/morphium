/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable.renderer;

import de.caluga.morphium.gui.recordtable.RecordTableState;

import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;

/**
 * @author stephan
 */
public abstract class RecordTableColumnRenderer implements TableCellEditor, TableCellRenderer {
    private RecordTableState state;

    public RecordTableColumnRenderer(RecordTableState initialState) {
        state = initialState;
    }

    public RecordTableState getTableState() {
        return state;
    }


}
