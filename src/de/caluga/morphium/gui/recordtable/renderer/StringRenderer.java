/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable.renderer;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ComponentAdapter;
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
    public Component getTableCellRendererComponent(JTable jtable,final Object o, final boolean selected, final boolean hasFocus, final int row, int col) {
        JLabel l = new JLabel();
        JXTable tx = (JXTable) jtable;
        Highlighter[] h = tx.getHighlighters();
        l.setText(o == null ? "" : o.toString());
        for (Highlighter hl:h) {

            hl.highlight(l,new ComponentAdapter(l) {
                @Override
                public Object getValueAt(int row, int column) {
                    return o;
                }

                @Override
                public boolean isCellEditable(int r, int column) {
                    return (row==0);
                }

                @Override
                public boolean hasFocus() {
                    return hasFocus;  //To change body of implemented methods use File | Settings | File Templates.
                }

                @Override
                public boolean isSelected() {
                    return selected;  //To change body of implemented methods use File | Settings | File Templates.
                }

                @Override
                public boolean isEditable() {
                    return row==0;  //To change body of implemented methods use File | Settings | File Templates.
                }
            });
        }
        if (selected) {
            l.setBackground(Color.blue);
        }

        return l;
    }


}
