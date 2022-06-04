package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;

import java.util.List;

public class WatchCmdSettings extends AggregateCmdSettings {
    private DriverTailableIterationCallback cb;

    public DriverTailableIterationCallback getCb() {
        return cb;
    }

    public WatchCmdSettings setCb(DriverTailableIterationCallback cb) {
        this.cb = cb;
        return this;
    }
}
