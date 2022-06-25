package de.caluga.morphium.driver.commands;

import de.caluga.morphium.Collation;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateMongoCommand extends WriteMongoCommand<UpdateMongoCommand> {
    private List<Map<String, Object>> updates;
    private Boolean ordered;
    private Map<String, Object> let;

    public UpdateMongoCommand(MorphiumDriver d) {
        super(d);
    }

    public UpdateMongoCommand addUpdate(Map<String, Object> upd) {
        if (updates == null) updates = new ArrayList<>();
        updates.add(upd);
        return this;
    }

    public List<Map<String, Object>> getUpdates() {
        return updates;
    }

    public UpdateMongoCommand setUpdates(List<Map<String, Object>> updates) {
        this.updates = updates;
        return this;
    }

    public Map<String, Object> getLet() {
        return let;
    }

    public UpdateMongoCommand setLet(Map<String, Object> let) {
        this.let = let;
        return this;
    }

    public Boolean getOrdered() {
        return ordered;
    }

    public UpdateMongoCommand setOrdered(Boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public UpdateMongoCommand addUpdate(Doc query, Doc update, Doc context, boolean upsert, boolean multi, Collation collation, List<Doc> arrayFilters, String hint) {
        if (updates == null) updates = new ArrayList<>();

        Doc upd = Doc.of("q", query, "u", update, "upsert", upsert, "multi", multi);
        if (context != null) upd.put("c", context);
        if (arrayFilters != null) upd.put("arrayFilters", arrayFilters);
        if (collation != null) upd.put("collation", collation.toQueryObject());
        if (hint != null) upd.put("hint", hint);

        updates.add(upd);
        return this;
    }

    @Override
    public String getCommandName() {
        return "update";
    }
}