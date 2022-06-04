package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.List;

public class FindAndModifyCmdSettings extends WriteCmdSettings<FindAndModifyCmdSettings> {
    private Doc query;
    private Doc sort;
    private boolean remove;
    private Doc update;
    private List<Doc> pipeline;
    private boolean newFlag;
    private boolean upsert;
    private Doc fields;
    private boolean bypassDocumentValidation;
    private Doc collation;
    private Object hint;
    private Doc let;

    public Doc getQuery() {
        return query;
    }

    public FindAndModifyCmdSettings setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Doc getSort() {
        return sort;
    }

    public FindAndModifyCmdSettings setSort(Doc sort) {
        this.sort = sort;
        return this;
    }

    public boolean isRemove() {
        return remove;
    }

    public FindAndModifyCmdSettings setRemove(boolean remove) {
        this.remove = remove;
        return this;
    }

    public Doc getUpdate() {
        return update;
    }

    public FindAndModifyCmdSettings setUpdate(Doc update) {
        this.update = update;
        return this;
    }

    public List<Doc> getPipeline() {
        return pipeline;
    }

    public FindAndModifyCmdSettings setPipeline(List<Doc> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public boolean isNewFlag() {
        return newFlag;
    }

    public FindAndModifyCmdSettings setNewFlag(boolean newFlag) {
        this.newFlag = newFlag;
        return this;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public FindAndModifyCmdSettings setUpsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public Doc getFields() {
        return fields;
    }

    public FindAndModifyCmdSettings setFields(Doc fields) {
        this.fields = fields;
        return this;
    }

    public boolean isBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public FindAndModifyCmdSettings setBypassDocumentValidation(boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public FindAndModifyCmdSettings setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public FindAndModifyCmdSettings setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public FindAndModifyCmdSettings setLet(Doc let) {
        this.let = let;
        return this;
    }
}
