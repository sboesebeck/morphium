package de.caluga.morphium.driver.bulk;/**
 * Created by stephan on 13.11.15.
 */

import de.caluga.morphium.driver.MorphiumDriver;

/**
 * TODO: Add Documentation here
 **/
public abstract class BulkRequest {
    private boolean ordered;

    public boolean isOrdered() {
        return ordered;
    }

    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    public abstract void execute(MorphiumDriver drv);
}
