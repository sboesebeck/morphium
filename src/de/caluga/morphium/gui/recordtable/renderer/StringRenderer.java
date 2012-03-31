/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable.renderer;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.Highlighter;

import javax.swing.*;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;
import java.awt.*;

/**
 * @author stephan
 */
public class StringRenderer extends DefaultCellEditor implements TableCellRenderer, TableCellEditor {

    public StringRenderer() {
        super(new JTextField());
    }

    @Override
    public Component getTableCellRendererComponent(JTable jtable, Object o, boolean selected, boolean hasFocus, int row, int col) {
        JLabel l = new JLabel();
        JXTable tx = (JXTable) jtable;
        Highlighter[] h = tx.getHighlighters();
        l.setText(o == null ? "" : o.toString());

        return l;
    }


}
