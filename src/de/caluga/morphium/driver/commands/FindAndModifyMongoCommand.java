package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;

import java.util.List;

public class FindAndModifyMongoCommand extends WriteMongoCommand<FindAndModifyMongoCommand> {
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

    public FindAndModifyMongoCommand(MorphiumDriver d) {
        super(d);
    }

    public Doc getQuery() {
        return query;
    }

    public FindAndModifyMongoCommand setQuery(Doc query) {
        this.query = query;
        return this;
    }

    public Doc getSort() {
        return sort;
    }

    public FindAndModifyMongoCommand setSort(Doc sort) {
        this.sort = sort;
        return this;
    }

    public boolean isRemove() {
        return remove;
    }

    public FindAndModifyMongoCommand setRemove(boolean remove) {
        this.remove = remove;
        return this;
    }

    public Doc getUpdate() {
        return update;
    }

    public FindAndModifyMongoCommand setUpdate(Doc update) {
        this.update = update;
        return this;
    }

    public List<Doc> getPipeline() {
        return pipeline;
    }

    public FindAndModifyMongoCommand setPipeline(List<Doc> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public boolean isNewFlag() {
        return newFlag;
    }

    public FindAndModifyMongoCommand setNewFlag(boolean newFlag) {
        this.newFlag = newFlag;
        return this;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public FindAndModifyMongoCommand setUpsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public Doc getFields() {
        return fields;
    }

    public FindAndModifyMongoCommand setFields(Doc fields) {
        this.fields = fields;
        return this;
    }

    public boolean isBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public FindAndModifyMongoCommand setBypassDocumentValidation(boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Doc getCollation() {
        return collation;
    }

    public FindAndModifyMongoCommand setCollation(Doc collation) {
        this.collation = collation;
        return this;
    }

    public Object getHint() {
        return hint;
    }

    public FindAndModifyMongoCommand setHint(Object hint) {
        this.hint = hint;
        return this;
    }

    public Doc getLet() {
        return let;
    }

    public FindAndModifyMongoCommand setLet(Doc let) {
        this.let = let;
        return this;
    }

    @Override
    public String getCommandName() {
        return "findAndModify";
    }
}
