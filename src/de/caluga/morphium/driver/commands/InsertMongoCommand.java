package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.List;
import java.util.Map;

public class InsertMongoCommand extends WriteMongoCommand<InsertMongoCommand> {
    private List<Map<String, Object>> documents;
    private Boolean ordered;
    private Boolean bypassDocumentValidation;
    private String comment;

    public InsertMongoCommand(MongoConnection d) {
        super(d);
    }

    public List<Map<String, Object>> getDocuments() {
        return documents;
    }

    public InsertMongoCommand setDocuments(List<Map<String, Object>> documents) {
        this.documents = documents;
        return this;
    }

    public Boolean getOrdered() {
        return ordered;
    }

    public InsertMongoCommand setOrdered(Boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public InsertMongoCommand setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public InsertMongoCommand setComment(String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public String getCommandName() {
        return "insert";
    }


    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        if (!getConnection().isConnected()) throw new RuntimeException("Not connected");
        Map<String, Object> writeResult = super.execute();
        if (writeResult.containsKey("writeErrors")) {
            int failedWrites = ((List) writeResult.get("writeErrors")).size();
            int success = (int) writeResult.get("n");
            throw new MorphiumDriverException("Failed to write: " + failedWrites + " - succeeded: " + success);
        }
        return writeResult;
    }
}
