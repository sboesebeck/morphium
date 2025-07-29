package de.caluga.morphium.config;

import de.caluga.morphium.MorphiumConfig;

public class CollectionCheckSettings {

    private MorphiumConfig.IndexCheck indexCheck = MorphiumConfig.IndexCheck.WARN_ON_STARTUP;
    private MorphiumConfig.CappedCheck cappedCheck = MorphiumConfig.CappedCheck.WARN_ON_STARTUP;
}
