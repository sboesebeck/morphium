package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.List;
import java.util.Map;

public class FindAndModifyMongoCommand extends WriteMongoCommand<FindAndModifyMongoCommand> {
    private Map<String, Object> query;
    private Map<String, Object> sort;
    private boolean remove;
    private Map<String, Object> update;
    private List<Map<String, Object>> pipeline;
    private boolean newFlag;
    private boolean upsert;
    private Map<String, Object> fields;
    private boolean bypassDocumentValidation;
    private Map<String, Object> collation;
    private Object hint;
    private Map<String, Object> let;

    public FindAndModifyMongoCommand(MongoConnection d) {
        super(d);
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public FindAndModifyMongoCommand setQuery(Map<String, Object> query) {
        this.query = query;
        return this;
    }

    public Map<String, Object> getSort() {
        return sort;
    }

    public FindAndModifyMongoCommand setSort(Map<String, Object> sort) {
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

    public Map<String, Object> getUpdate() {
        return update;
    }

    public FindAndModifyMongoCommand setUpdate(Map<String, Object> update) {
        this.update = update;
        return this;
    }

    public List<Map<String, Object>> getPipeline() {
        return pipeline;
    }

    public FindAndModifyMongoCommand setPipeline(List<Map<String, Object>> pipeline) {
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

    public Map<String, Object> getFields() {
        return fields;
    }

    public FindAndModifyMongoCommand setFields(Map<String, Object> fields) {
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

    public Map<String, Object> getCollation() {
        return collation;
    }

    public FindAndModifyMongoCommand setCollation(Map<String, Object> collation) {
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

    public Map<String, Object> getLet() {
        return let;
    }

    public FindAndModifyMongoCommand setLet(Map<String, Object> let) {
        this.let = let;
        return this;
    }

    @Override
    public String getCommandName() {
        return "findAndModify";
    }

    public Map<String,Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException{
        ExplainCommand explainCommand = new ExplainCommand(getConnection());
        explainCommand.setVerbosity(verbosity);
        var m=asMap();
        m.remove("$db");
        m.remove("coll");
        explainCommand.setCommand(m);
        explainCommand.setDb(getDb()).setColl(getColl());
        int msg=explainCommand.executeAsync();
        return explainCommand.getConnection().readSingleAnswer(msg);

    }

    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        var writeResult = super.execute();
        if (writeResult.containsKey("writeErrors")) {
            int failedWrites = ((List) writeResult.get("writeErrors")).size();
            int success = (int) writeResult.get("n");
            throw new RuntimeException("Failed to write: " + failedWrites + " - succeeded: " + success);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) writeResult.get("value");
        return value;
    }
}
