package de.caluga.morphium.driver.commands;

import de.caluga.morphium.Collation;
import de.caluga.morphium.driver.Doc;

import java.util.ArrayList;
import java.util.List;

public class UpdateCmdSettings extends WriteCmdSettings<UpdateCmdSettings> {
    private List<Doc> updates;
    private Boolean ordered;
    private Doc let;

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

    public Doc getLet() {
        return let;
    }

    public UpdateCmdSettings setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Boolean getOrdered() {
        return ordered;
    }

    public UpdateCmdSettings setOrdered(Boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public UpdateCmdSettings addUpdate(Doc query, Doc update, Doc context, boolean upsert, boolean multi, Collation collation, List<Doc> arrayFilters, String hint) {
        if (updates == null) updates = new ArrayList<>();

        Doc upd = Doc.of("q", query, "u", update, "upsert", upsert, "multi", multi);
        if (context != null) upd.put("c", context);
        if (arrayFilters != null) upd.put("arrayFilters", arrayFilters);
        if (collation != null) upd.put("collation", collation.toQueryObject());
        if (hint != null) upd.put("hint", hint);

        updates.add(upd);
        return this;
    }
}
