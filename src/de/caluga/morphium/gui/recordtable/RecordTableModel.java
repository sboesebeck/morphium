/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Query;
import org.apache.log4j.Logger;
import org.jdesktop.swingx.JXTable;

import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumnModel;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.*;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author stephan
 */
public class RecordTableModel<T> extends AbstractTableModel {

    /**
     *
     */
    private static final long serialVersionUID = -121394722992680297L;
    private Class<T> recordClass;
    private List<T> data;
    private JTableHeader tableHeader;
    public static final int DESCENDING = -1;
    public static final int NOT_SORTED = 0;
    public static final int ASCENDING = 1;
    private static Directive EMPTY_DIRECTIVE = new Directive(-1, NOT_SORTED);
    private List<Directive> sortingColumns = new ArrayList<Directive>();
    private MouseListener mouseListener;
    private TreeMap<String, Integer> sorting;
    private JXTable table;
    private RecordTableState initialState;
    private static Logger log = Logger.getLogger(RecordTableModel.class);

    private Map<String, ColumnDataRenderer> rendererMap;

    public RecordTableModel(Class<T> cls, RecordTableState initial) {
        recordClass = cls;
        initialState = initial;

        if (initial.isPreCacheAll()) {
            data = Morphium.get().find(initial.getSearch());
            log.info("Got Elements: " + data.size());
        } else {
            data = new ArrayList<T>();
        }

        mouseListener = new MouseHandler();
    }

    public Map<String, ColumnDataRenderer> getRendererMap() {
        return rendererMap;
    }

    public void setRendererMap(Map<String, ColumnDataRenderer> rendererMap) {
        this.rendererMap = rendererMap;
    }

    public List<T> getData() {
        return data;
    }

    public long getElementCount() {
        return initialState.getSearch().countAll();
    }


    public JXTable getTable() {
        return table;
    }

    public void setTable(JXTable table) {
        this.table = table;
    }

    public boolean isSearchable() {
        return initialState.isSearchable();
    }

    public void setSearchable(boolean searchable) {
        if (searchable != initialState.isSearchable()) {
            initialState.setSearchable(searchable);
            fireTableStructureChanged();
        }
    }

    private String getOrderString() {
        StringBuilder b = new StringBuilder();
        String del = "";
        for (String fld : sorting.keySet()) {
            if (sorting.get(fld) < 0) {
                b.append('-');
            }
            b.append(fld);
            b.append(del);
            del = ",";
        }
        return b.toString();
    }


    public void updateModel() {
        Query search = initialState.getSearch();
        if (sorting != null) {
            if (sorting.size() != 0) {
                search = search.order(getOrderString());
            }
        }
        if (initialState.isPaging()) {
            search = search.skip(initialState.getCurrentPage() * initialState.getPageLength()).limit(initialState.getPageLength());
        }
        data = Morphium.get().find(search);
        fireTableDataChanged();
    }

    public void setTableHeader(JTableHeader tableHeader) {
        if (this.tableHeader != null) {
            this.tableHeader.removeMouseListener(mouseListener);
            TableCellRenderer defaultRenderer = this.tableHeader.getDefaultRenderer();
            if (defaultRenderer.getClass().equals(SortableHeaderRenderer.class)) {
                this.tableHeader.setDefaultRenderer(((SortableHeaderRenderer) defaultRenderer).tableCellRenderer);
            }
        }
        this.tableHeader = tableHeader;
        if (this.tableHeader != null) {
            this.tableHeader.addMouseListener(mouseListener);
            this.tableHeader.setDefaultRenderer(new SortableHeaderRenderer(this.tableHeader.getDefaultRenderer()));
        }
    }

    public T getRecord(int row) {
        return data.get(row);
    }

    @Override
    public int getColumnCount() {
        log.info("Showing " + initialState.getFieldsToShow().size());
        return initialState.getFieldsToShow().size();
    }

    @Override
    public String getColumnName(int column) {
        log.info("Columname " + column + " = " + initialState.getColumnName(column));
        return initialState.getColumnName(column);
    }

    @Override
    public int getRowCount() {
        if (isSearchable()) {
            return data.size() + 1;
        }
        return data.size();
    }

    @Override
    public Object getValueAt(int row, int col) {
        if (isSearchable()) {
            if (row == 0) {
                if (initialState.isSearchable(col)) {
                    if ((initialState.getSearchValues().get((String) initialState.getFieldsToShow().get(col))) == null) {
//                        log.info("Search value for " + ((String) initialState.getFieldsToShow().get(col)) + " is null");

                        return null;
                    }
                    return (initialState.getSearchValues().get((String) initialState.getFieldsToShow().get(col))).toString().replaceAll(".\\*", "*").replaceAll("\\^", "").replaceAll("\\$", "")
                            .replaceAll("[{}:\"]", "")

                            .replaceAll(",", "");
                } else {
                    return ""; //not searchable
                }
            }
            row--;
        }
        if (row > data.size()) {
            return "";
        }
        T r = data.get(row);
        if (rendererMap != null) {
            if (rendererMap.get(initialState.getFieldsToShow().get(col)) != null) {
                log.debug("Found renderer");
                return rendererMap.get(initialState.getFieldsToShow().get(col)).renderValueFor((String) initialState.getFieldsToShow().get(col), r);
            }
            for (String fld : rendererMap.keySet()) {
                Pattern p = Pattern.compile(fld);
                if (p.matcher((String) initialState.getFieldsToShow().get(col)).matches()) {
                    log.debug("Found renderer with RegEx");
                    return rendererMap.get(initialState.getFieldsToShow().get(col)).renderValueFor((String) initialState.getFieldsToShow().get(col), r);
                }
            }
        }
        try {
            return Morphium.get().getValue(r, (String) initialState.getFieldsToShow().get(col));
        } catch (IllegalAccessException e) {
            log.fatal("Illegal Access for Value " + row + "," + col + " (" + initialState.getFieldsToShow().get(col) + " of object " + r.getClass().getSimpleName());
            return "ERR";
        }
    }

    @Override
    public boolean isCellEditable(int row, int column) {
        if (isSearchable()) {
            if (row == 0) {
                log.info("Returning true for row " + row + " and col " + column);
                return true;
            }
            row--;
        }

        return false;
    }

    @Override
    public void setValueAt(Object aValue, int row, int column) {
        if (isSearchable()) {
            if (row == 0) {
                String txt = aValue.toString();
                //Parsing
                initialState.removeSearchForCol((String) initialState.getFieldsToShow().get(column));
                if (txt.trim().equals("")) {
                    //do nothing
                } else {
                    Map<String, Object> obj = new HashMap<String, Object>();

                    Class type = initialState.getType();
                    Class fldType = Morphium.get().getTypeOfField(type, (String) initialState.getFieldsToShow().get(column));
                    String fldName = (String) initialState.getFieldsToShow().get(column);
                    if (fldType.equals(String.class)) {
                        if (txt.contains("*")) {
                            txt = txt.replaceAll("\\*", ".*");
                            Pattern p = Pattern.compile("^" + txt + "$");
                            obj.put(fldName, p);
                        } else {
                            obj.put(fldName, txt);
                        }
                    } else if (fldType.equals(Long.class) || fldType.equals(long.class)) {
                        log.info("Type ist Long");
                        obj.put(fldName, Long.valueOf(txt));
                    } else if (fldType.equals(Integer.class) || fldType.equals(int.class)) {
                        log.info("type is int");
                        obj.put(fldName, Integer.valueOf(txt));
                        log.info("Set value!" + Integer.getInteger(txt));
                    } else if (fldType.equals(Double.class) || fldType.equals(double.class)) {
                        obj.put(fldName, Double.valueOf(txt));
                    } else if (fldType.equals(Boolean.class) || fldType.equals(boolean.class)) {
                        obj.put(fldName, Boolean.valueOf(txt));
                    } else {
                        log.fatal("Unsupported type for searching");
                        JOptionPane.showMessageDialog(null, "Feld " + fldName + " kann nicht durchsucht werden...");
//                    } else {
//                        
//                        obj.put(fldName, txt);
                    }
                    initialState.setSearchValues(obj);
                }

//                log.info("Setting search Value to " + initialState.getSearchValues().toString());
                updateModel();
                fireTableDataChanged();
                return;
            }
            row--;
        }
        try {
            Morphium.get().setValue(data.get(row), (String) initialState.getFieldsToShow().get(column), aValue);
        } catch (IllegalAccessException e) {
            log.fatal("Error setting value",e);
        }
    }

    private Object getNumberObject(String txt) throws HeadlessException {
        //Try numbers
        Object v = null;
        try {
            v = new Integer(txt);
        } catch (Exception e) {
            try {
                v = new Long(txt);
            } catch (Exception ex) {
                try {
                    v = new Double(txt);
                } catch (Exception exx) {
                    JOptionPane.showMessageDialog(null, "Error, number Format " + txt);
                    log.warn("Error:" + exx.getMessage(), exx);
                }
            }
        }
        return v;
    }

    @Override
    public Class getColumnClass(int columnIndex) {
        String fld = (String) initialState.getFieldsToShow().get(columnIndex);
//        log.info("Returning " + Morphium.get().getTypeOfField(recordClass, fld).getName() + " for col " + columnIndex);
        return Morphium.getTypeOfField(recordClass, fld);
    }

    private void clearSortingState() {
        sorting = new TreeMap<String, Integer>();

    }

    private void cancelSorting() {
        clearSortingState();
        sortingColumns.clear();
        sortingStatusChanged();
    }

    private void sortingStatusChanged() {
        clearSortingState();
        //Creating sortingMap
        for (Directive d : sortingColumns) {
            if (d.direction != NOT_SORTED) {
                sorting.put((String) initialState.getFieldsToShow().get(d.column), d.direction);
            }
        }
        updateModel();

        fireTableDataChanged();
        if (tableHeader != null) {
            tableHeader.repaint();
        }
    }

    public int getSortingStatus(int column) {
        return getDirective(column).direction;
    }

    private Directive getDirective(int column) {
        for (Directive directive : sortingColumns) {
            if (directive.column == column) {
                return directive;
            }
        }
        return EMPTY_DIRECTIVE;
    }

    public void setSortingStatus(int column, int status) {
        Directive directive = getDirective(column);
        if (directive != EMPTY_DIRECTIVE) {
            sortingColumns.remove(directive);
        }
        if (status != NOT_SORTED) {
            sortingColumns.add(new Directive(column, status));
        }
        sortingStatusChanged();
    }

    protected Icon getHeaderRendererIcon(int column, int size) {
        Directive directive = getDirective(column);
        if (directive == EMPTY_DIRECTIVE) {
            return null;
        }
        return new Arrow(directive.direction == DESCENDING, size * 2, sortingColumns.indexOf(directive));
    }


    private class MouseHandler extends MouseAdapter {

        public void mouseClicked(MouseEvent e) {
            JTableHeader h = (JTableHeader) e.getSource();
            TableColumnModel columnModel = h.getColumnModel();
            int viewColumn = columnModel.getColumnIndexAtX(e.getX());
            int column = columnModel.getColumn(viewColumn).getModelIndex();
            if (column != -1) {
                int status = getSortingStatus(column);
                if (!e.isControlDown()) {
                    cancelSorting();
                }
                // Cycle the sorting states through {NOT_SORTED, ASCENDING, DESCENDING}
                // or
                // {NOT_SORTED, DESCENDING, ASCENDING} depending on whether shift is
                // pressed.
                status = status + (e.isShiftDown() ? -1 : 1);
                status = (status + 4) % 3 - 1; // signed mod, returning {-1, 0, 1}
                setSortingStatus(column, status);
            }
        }
    }

    private static class Arrow implements Icon {

        private boolean descending;
        private int size;
        private int priority;

        public Arrow(boolean descending, int size, int priority) {
            this.descending = descending;
            this.size = size;
            this.priority = priority;
        }

        public void paintIcon(Component c, Graphics g, int x, int y) {
            Color color = c == null ? Color.GRAY : c.getBackground();
            // In a compound sort, make each succesive triangle 20%
            // smaller than the previous one.
            int dx = (int) (size / 2 * Math.pow(0.8, priority));
            int dy = descending ? dx : -dx;
            // Align icon (roughly) with font baseline.
            y = y + 5 * size / 6 + (descending ? -dy : 0);
            int shift = descending ? 1 : -1;
            g.translate(x, y);
            // Right diagonal.
            g.setColor(color.darker().darker());

            g.drawLine(dx / 2, dy, 0, 0);
            g.drawLine(dx / 2, dy + shift, 0, shift);
            // Left diagonal.
            g.setColor(color.brighter());
            g.drawLine(dx / 2, dy, dx, 0);
            g.drawLine(dx / 2, dy + shift, dx, shift);
            // Horizontal line.
            if (descending) {
                g.setColor(color.darker().darker().darker());
            } else {
                g.setColor(color.brighter().brighter().brighter());
            }
            g.drawLine(dx, 0, 0, 0);
            g.setColor(color);
            g.translate(-x, -y);
        }

        public int getIconWidth() {
            return size;
        }

        public int getIconHeight() {
            return size;
        }
    }

    private class SortableHeaderRenderer implements TableCellRenderer {

        private TableCellRenderer tableCellRenderer;

        public SortableHeaderRenderer(TableCellRenderer tableCellRenderer) {
            this.tableCellRenderer = tableCellRenderer;
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
                                                       boolean hasFocus, int row, int column) {
            Component c = tableCellRenderer.getTableCellRendererComponent(table, value, isSelected,
                    hasFocus, row, column);
            if (c instanceof JLabel) {
                JLabel l = (JLabel) c;
                l.setHorizontalTextPosition(JLabel.LEFT);
                int modelColumn = table.convertColumnIndexToModel(column);
                l.setIcon(getHeaderRendererIcon(modelColumn, l.getFont().getSize()));
            }
            return c;
        }
    }

    private static class Directive {

        private int column;
        private int direction;

        public Directive(int column, int direction) {
            this.column = column;
            this.direction = direction;
        }
    }
}
