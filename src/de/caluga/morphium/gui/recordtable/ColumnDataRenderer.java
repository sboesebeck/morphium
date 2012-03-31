package de.caluga.morphium.gui.recordtable;

/**
 * User: Stephan Bösebeck
 * Date: 24.03.12
 * Time: 16:31
 * <p/>
 * used to render special values for RecordTable without the need of creating a different Render component
 * e.g. indirect fields / References... lookups etc.
 */
public interface ColumnDataRenderer {

    public String renderValueFor(String value, Object o);
}
