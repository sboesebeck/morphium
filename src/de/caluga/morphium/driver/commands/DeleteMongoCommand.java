package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;

import java.util.ArrayList;
import java.util.List;

public class DeleteMongoCommand extends WriteMongoCommand<DeleteMongoCommand> {
    private List<Doc> deletes;
    private Doc let;
    private Boolean ordered;

    public DeleteMongoCommand(MorphiumDriver d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "delete";
    }

    public DeleteMongoCommand addDelete(Doc del) {
        if (deletes == null) deletes = new ArrayList<>();
        deletes.add(del);
        return this;
    }

    public List<Doc> getDeletes() {
        return deletes;
    }

    public DeleteMongoCommand setDeletes(List<Doc> deletes) {
        this.deletes = deletes;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public DeleteMongoCommand setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Boolean isOrdered() {
        return ordered;
    }

    public DeleteMongoCommand setOrdered(Boolean ordered) {
        this.ordered = ordered;
        return this;
    }


    public DeleteMongoCommand addDelete(Doc query, Integer limit, Doc collation, String hint) {
        if (deletes == null) deletes = new ArrayList<>();

        Doc del = Doc.of("q", query);
        if (limit != null) del.put("limit", limit);
        if (collation != null) del.put("collation", collation);
        if (hint != null) del.put("hint", hint);

        deletes.add(del);
        return this;
    }


}
