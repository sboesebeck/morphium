package de.caluga.morphium.gui.recordtable.renderer;

import de.caluga.morphium.gui.recordtable.RecordTableState;
import org.apache.log4j.Logger;

import javax.swing.*;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.EventObject;
import java.util.Vector;

/**
 * @author stephan
 *         <p/>
 */
public class BooleanRenderer extends RecordTableColumnRenderer implements TableCellRenderer, TableCellEditor {

    private JCheckBox bx;
    private Vector listeners = new Vector();
    private static Logger log = Logger.getLogger(BooleanRenderer.class);

    public BooleanRenderer(RecordTableState state) {
        super(state);
    }
    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.table.TableCellRenderer#getTableCellRendererComponent(javax.swing.JTable,
     *      java.lang.Object, boolean, boolean, int, int)
     */

    public Component getTableCellRendererComponent(final JTable arg0, Object val, boolean selected,
                                                   boolean has_focus, final int row, final int col) {
        Pos p = new Pos(row, col);
        if ((getTableState().isSearchable((String) getTableState().getFieldsToShow().get(col)) && row == 0)) {
            //searcher
            bx = new JCheckBox();
            bx.setOpaque(true);
            if (val instanceof Boolean) {
                bx.setSelected((Boolean) val);
            } else if (val == null) {
                bx.setSelected(false);
            } else {
                bx.setSelected(val.toString().equalsIgnoreCase("true"));
            }
            if (selected) {
                bx.setBackground(arg0.getSelectionBackground());
                bx.setForeground(arg0.getSelectionForeground());
            }
            bx.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    stopCellEditing();
                }
            });
            return bx;
        }
        if (getTableState().isEditable()) {
            bx = new JCheckBox();
            bx.setOpaque(true);
            bx.setSelected((Boolean) val);
            if (selected) {
                bx.setBackground(arg0.getSelectionBackground());
                bx.setForeground(arg0.getSelectionForeground());
            }
            bx.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    stopCellEditing();
                }
            });
            return bx;
        }
        if (val == null) return new JLabel("n/a");
        if (val.equals(Boolean.FALSE)) return new JLabel("Nein");
        if (val.equals(Boolean.TRUE)) return new JLabel("Ja");
        return new JLabel();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.table.TableCellEditor#getTableCellEditorComponent(javax.swing.JTable,
     *      java.lang.Object, boolean, int, int)
     */
    public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected,
                                                 int row, int column) {
        return getTableCellRendererComponent(table, value, isSelected, false, row, column);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.CellEditor#getCellEditorValue()
     */
    public Object getCellEditorValue() {
        return bx.isSelected();
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.CellEditor#isCellEditable(java.util.EventObject)
     */
    public boolean isCellEditable(EventObject anEvent) {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.CellEditor#shouldSelectCell(java.util.EventObject)
     */
    public boolean shouldSelectCell(EventObject anEvent) {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.CellEditor#stopCellEditing()
     */
    public boolean stopCellEditing() {
        System.out.println("stop");
        for (Object listener : listeners) {
            ((CellEditorListener) listener).editingStopped(new ChangeEvent(this));
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.CellEditor#cancelCellEditing()
     */
    public void cancelCellEditing() {
        log.warn("Cancel called - don't know what to do");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.CellEditor#addCellEditorListener(javax.swing.event.CellEditorListener)
     */
    public void addCellEditorListener(CellEditorListener l) {

        listeners.add(l);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.swing.CellEditor#removeCellEditorListener(javax.swing.event.CellEditorListener)
     */
    public void removeCellEditorListener(CellEditorListener l) {
        listeners.remove(l);
    }

    private class Pos {

        int row, col;

        public Pos(int r, int c) {
            row = r;
            col = c;
        }
    }
}
