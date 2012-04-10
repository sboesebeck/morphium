package de.caluga.morphium;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 10.04.12
 * Time: 22:26
 * <p/>
 * Morphium honors entities, which implement this interface in a special way: only those fields are updated, which are
 * returned by getAlteredFields - if no fields are returned, no update takes place!
 */
public interface PartiallyUpdateable {
    List<String> getAlteredFields();
}
