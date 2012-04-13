/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable.renderer;

import de.caluga.morphium.gui.recordtable.RecordTableState;
import org.jdesktop.swingx.JXLabel;
import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.Highlighter;

import javax.swing.*;
import javax.swing.event.CellEditorListener;
import java.awt.*;
import java.util.EventObject;

/**
 * @author stephan
 */
public class NumberRenderer extends RecordTableColumnRenderer {
    private JTextField input=new JTextField();
    private Class<?> lastType;
    public NumberRenderer(RecordTableState initialState) {
        super(initialState);
    }

    @Override
    public Component getTableCellEditorComponent(JTable jtable, Object value, boolean isSelected, int row, int col) {
        if (value!=null) {
            input.setText(value.toString());
            lastType=value.getClass();
        }

       return input;
    }

    @Override
    public Object getCellEditorValue() {
        if (lastType==null) {
            lastType=String.class;
        }
        if (input.getText().equals("")) {
            return 0;
        }
        if (lastType.equals(String.class)) {
            return input.getText();
        }
        if (lastType.equals(Long.class) || lastType.equals(long.class)) {
            return Long.valueOf(input.getText());
        }
        if (lastType.equals(Integer.class) || lastType.equals(int.class)) {
            return Integer.valueOf(input.getText());
        }
        if (lastType.equals(Float.class) || lastType.equals(float.class)) {
            return Float.valueOf(input.getText());
        }
        if (lastType.equals(Double.class) || lastType.equals(double.class)) {
            return Double.valueOf(input.getText());
        }
        return input.getText();
    }

    @Override
    public boolean isCellEditable(EventObject eo) {
        return true;
    }

    @Override
    public boolean shouldSelectCell(EventObject eo) {
        return true;
    }

    @Override
    public boolean stopCellEditing() {
//        input.setText("");
        return true;
    }

    @Override
    public void cancelCellEditing() {
        input.setText("");
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
        JXLabel l = new JXLabel();
        JXTable tx = (JXTable) jtable;
        Highlighter[] h = tx.getHighlighters();
        l.setText(o == null ? "" : o.toString());
        for (Highlighter hl:h) {
            hl.highlight(l,null);
        }
        return l;
    }

}
