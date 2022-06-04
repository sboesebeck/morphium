package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.ArrayList;
import java.util.List;

public class UpdateCmdSettings extends WriteCmdSettings<UpdateCmdSettings> {
    private List<Doc> updates;
    private Boolean ordered;

    public UpdateCmdSettings addUpdate(Doc upd) {
        if (updates == null) updates = new ArrayList<>();
        updates.add(upd);
        return this;
    }

    public List<Doc> getUpdates() {
        return updates;
    }

    public UpdateCmdSettings setUpdates(List<Doc> updates) {
        this.updates = updates;
        return this;
    }

    public Boolean getOrdered() {
        return ordered;
    }

    public UpdateCmdSettings setOrdered(Boolean ordered) {
        this.ordered = ordered;
        return this;
    }
}
