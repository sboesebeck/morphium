package de.caluga.morphium.cache;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamListener;

public class CacheSyncWatcher implements ChangeStreamListener {

    private Morphium morphium;

    public CacheSyncWatcher(Morphium m) {
        morphium = m;
    }


    @Override
    public boolean incomingData(ChangeStreamEvent evt) {
        return false;
    }
}
