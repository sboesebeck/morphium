package de.caluga.morphium;

import java.util.List;

/**
 * User: Stpehan Bösebeck
 * Date: 11.04.12
 * Time: 08:41
 * <p/>
 */
public interface PartiallyUpdateable {
    List<String> getAlteredFields();

    void clearAlteredFields();
}
