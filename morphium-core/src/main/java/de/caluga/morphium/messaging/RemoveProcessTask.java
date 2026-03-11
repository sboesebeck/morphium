package de.caluga.morphium.messaging;

import de.caluga.morphium.driver.MorphiumId;

import java.util.List;
import java.util.Set;

public class RemoveProcessTask implements Runnable {
    @SuppressWarnings("CanBeFinal")
    Set<MorphiumId> processing;
    @SuppressWarnings("CanBeFinal")
    MorphiumId toRemove;

    public RemoveProcessTask(Set<MorphiumId> processing, MorphiumId toRemove) {
        this.processing = processing;
        this.toRemove = toRemove;
    }

    @Override
    public void run() {
        processing.remove(toRemove);
    }
}
