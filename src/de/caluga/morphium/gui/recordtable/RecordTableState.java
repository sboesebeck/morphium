/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.gui.recordtable;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Query;
import org.apache.log4j.Logger;

import java.awt.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author stephan
 */
public class RecordTableState<T> {
    private String mongoId;
    private List<String> fieldsToShow;
    private List<String> columnHeader;
    private Query<T> initialSearch;
    private List<String> fieldsToSearchFor; //in template!

    //fields that can be searched
    private List<String> searchableFields;
    //values for fields to search
    private Map<String, Object> searchValues;

    private Map<String, RecordTableColumnTypes> displayTypeForField;
    private Map<String, ColumnDataRenderer> rendererMap;

    private List<AbstractRecMenuItem> menuItemList;


    private boolean preCacheAll;
    private boolean paging;
    private int currentPage;
    private int pageLength;


    private boolean editable;
    private boolean deleteable;
    private boolean searchable;
    private static Logger logger = Logger.getLogger(RecordTableState.class);

    private Class<T> type;

    public RecordTableState(Class<T> cls) {
        type = cls;
        preCacheAll = true;
        paging = false;
        rendererMap = new HashMap<String, ColumnDataRenderer>();
        menuItemList=new ArrayList<AbstractRecMenuItem>();
    }

    public List<AbstractRecMenuItem> getMenuItemList() {
        return menuItemList;
    }

    public Map<String, ColumnDataRenderer> getRendererMap() {
        return rendererMap;
    }

    public void addMenuItem(AbstractRecMenuItem mi) {
        menuItemList.add(mi);
    }

    public void removeMenuItem(AbstractRecMenuItem mi) {
        menuItemList.remove(mi);
    }

    /**
     * @param fld - may also be a regular Expression!!!
     * @param rd  - the renderer
     */
    public void addRendererForField(String fld, ColumnDataRenderer rd) {
        if (rendererMap.get(fld) != null) {
            throw new IllegalArgumentException("Renderer for Field " + fld + " already set - remove first!");
        }
        rendererMap.put(fld, rd);
    }

    public void removeRendererForField(String fld) {
        rendererMap.remove(fld);
    }

    public boolean isDeleteable() {
        return deleteable;
    }

    public void setDeleteable(boolean deleteable) {
        this.deleteable = deleteable;
    }

    public Class<T> getType() {
        return type;
    }

    public void setType(Class<T> type) {
        this.type = type;
    }


    public boolean isSearchable() {
        return searchable;
    }

    public void setSearchable(boolean searchable) {
        this.searchable = searchable;
    }

    public Map<String, Object> getSearchValues() {
        if (searchValues == null)
            searchValues = new HashMap<String, Object>();
        return searchValues;
    }


    public void setSearchValues(Map<String, Object> searchValues) {
        if (this.searchValues == null) {
            this.searchValues = searchValues;
        } else {
            for (String s : searchValues.keySet()) {
                this.searchValues.put(s, searchValues.get(s));
            }
        }

    }

    public void removeSearchForCol(String fld) {
        if (this.searchValues == null) return;
        searchValues.remove(fld);

    }

    public Query<T> getSearch() {
        Query<T> ret = null;
        if (initialSearch == null) {
            ret = Morphium.get().createQueryFor(type);
        } else {
            ret = initialSearch;
        }
        if (searchValues != null) {
            for (String s : searchValues.keySet()) {
                if (searchValues.get(s) != null) {
//                    String s1 = searchValues.get(s).toString();
                    //TODO: add Filter support
                    //TODO: make better type matching here
                    logger.warn("Filter support missing! Type Mathing not complete yet!!!");
                    Class fldType = Morphium.get().getTypeOfField(type, s);

                    ret.f(s).eq(searchValues.get(s));

                }
            }
        }
        if (paging) {
            ret = ret.skip(currentPage * pageLength).limit(pageLength);
        }
        return ret;
    }


    public boolean isEditable() {
        return editable;
    }

    public void setEditable(boolean editable) {
        this.editable = editable;
    }


    //convinience Methods
    public String getColumnName(int col) {
        if (columnHeader == null || columnHeader.size() < col) {
            return getFieldsToShow().get(col);
            //return "Col"+col;
        }
        return columnHeader.get(col);
    }


    ///Getter + Setter ab hier
    public int getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public int getPageLength() {
        if (pageLength == 0) {
            pageLength = 10;
        }
        return pageLength;
    }

    public void setPageLength(int pageLength) {
        this.pageLength = pageLength;
    }

    public boolean isPaging() {
        return paging;
    }

    public void setPaging(boolean paging) {
        this.paging = paging;
    }

    public boolean isSearchable(int fldIdx) {
        return (isSearchable(getFieldsToShow().get(fldIdx)));
    }

    public boolean isSearchable(String fld) {
        if (searchableFields == null) {
            searchableFields = fieldsToShow;
        }
        return searchableFields.contains(fld);
    }

    public boolean isPreCacheAll() {
        return preCacheAll;
    }

    public void setPreCacheAll(boolean preCacheAll) {
        this.preCacheAll = preCacheAll;
    }


    public List<String> getColumnHeader() {
        return columnHeader;
    }

    public void setColumnHeader(List<String> columnHeader) {
        this.columnHeader = columnHeader;
    }

    public Map<String, RecordTableColumnTypes> getDisplayTypeForField() {
        return displayTypeForField;
    }

    public void setDisplayTypeForField(Map<String, RecordTableColumnTypes> displayTypeForField) {
        this.displayTypeForField = displayTypeForField;
    }

    public List<String> getFieldsToSearchFor() {
        return fieldsToSearchFor;
    }

    public void setFieldsToSearchFor(List<String> fieldsToSearchFor) {
        this.fieldsToSearchFor = fieldsToSearchFor;
    }

    public List<String> getFieldsToShow() {
        if (fieldsToShow == null) {
            fieldsToShow = Morphium.getFields(type);
        }
        return fieldsToShow;
    }

    public void setFieldsToShow(List<String> fieldsToShow) {
        this.fieldsToShow = fieldsToShow;
    }

    public Query<T> getInitialSearch() {
        return initialSearch;
    }

    public void setInitialSearch(Query<T> initialSearch) {
        this.initialSearch = initialSearch;
    }

    public List<String> getSearchableFields() {
        return searchableFields;
    }

    public void setSearchableFields(List<String> searchableFields) {
        this.searchableFields = searchableFields;
    }


}
