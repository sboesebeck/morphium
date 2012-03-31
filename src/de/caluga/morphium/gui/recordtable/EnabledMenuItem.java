package de.caluga.morphium.gui.recordtable;

/**
 * User: Stephan Bösebeck
 * Date: 31.03.12
 * Time: 16:09
 * <p/>
 * TODO: Add documentation here
 */
public class EnabledMenuItem extends AbstractRecMenuItem {
    @Override
    public boolean isEnabled(Object selectedRecord) {
        return true;
    }
}
