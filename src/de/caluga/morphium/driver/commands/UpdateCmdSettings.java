package de.caluga.morphium.driver.commands;

import de.caluga.morphium.Collation;
import de.caluga.morphium.driver.Doc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateCmdSettings extends WriteCmdSettings<UpdateCmdSettings> {
    private List<Map<String, Object>> updates;
    private Boolean ordered;
    private Map<String, Object> let;

    public UpdateCmdSettings addUpdate(Map<String, Object> upd) {
        if (updates == null) updates = new ArrayList<>();
        updates.add(upd);
        return this;
    }

    public List<Map<String, Object>> getUpdates() {
        return updates;
    }

    public UpdateCmdSettings setUpdates(List<Map<String, Object>> updates) {
        this.updates = updates;
        return this;
    }

    public Map<String, Object> getLet() {
        return let;
    }

    public UpdateCmdSettings setLet(Map<String, Object> let) {
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
