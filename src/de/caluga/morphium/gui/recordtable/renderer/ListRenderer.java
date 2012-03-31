/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable.renderer;

import de.caluga.morphium.gui.recordtable.RecordTableState;

import javax.swing.*;
import javax.swing.event.CellEditorListener;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;
import java.awt.*;
import java.util.EventObject;

/**
 * @author stephan
 */
public class ListRenderer extends RecordTableColumnRenderer implements TableCellEditor, TableCellRenderer {
    public ListRenderer(RecordTableState s) {
        super(s);
    }

    @Override
    public Component getTableCellEditorComponent(JTable jtable, Object o, boolean bln, int i, int i1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getCellEditorValue() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isCellEditable(EventObject eo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean shouldSelectCell(EventObject eo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean stopCellEditing() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void cancelCellEditing() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addCellEditorListener(CellEditorListener cl) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeCellEditorListener(CellEditorListener cl) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Component getTableCellRendererComponent(JTable jtable, Object o, boolean bln, boolean bln1, int i, int i1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
