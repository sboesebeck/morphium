package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.ArrayList;
import java.util.List;

public class DeleteCmdSettings extends WriteCmdSettings<DeleteCmdSettings> {
    private List<Doc> deletes;
    private Doc let;
    private Boolean ordered;

    public DeleteCmdSettings addDelete(Doc del) {
        if (deletes == null) deletes = new ArrayList<>();
        deletes.add(del);
        return this;
    }

    public List<Doc> getDeletes() {
        return deletes;
    }

    public DeleteCmdSettings setDeletes(List<Doc> deletes) {
        this.deletes = deletes;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public DeleteCmdSettings setLet(Doc let) {
        this.let = let;
        return this;
    }

    public Boolean isOrdered() {
        return ordered;
    }

    public DeleteCmdSettings setOrdered(Boolean ordered) {
        this.ordered = ordered;
        return this;
    }
}
