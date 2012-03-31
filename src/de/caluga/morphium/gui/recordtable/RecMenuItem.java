package de.caluga.morphium.gui.recordtable;

/**
 * User: Stephan Bösebeck
 * Date: 31.03.12
 * Time: 16:08
 * <p/>
 * TODO: Add documentation here
 */
public class RecMenuItem extends AbstractRecMenuItem{
    @Override
    public boolean isEnabled(Object selectedRecord) {
        return selectedRecord!=null;
    }
}
