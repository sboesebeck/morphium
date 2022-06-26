package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver;

import java.util.List;
import java.util.Map;

public class InsertMongoCommand extends WriteMongoCommand<InsertMongoCommand> {
    private List<Map<String, Object>> documents;
    private Boolean ordered;
    private Boolean bypassDocumentValidation;
    private String comment;

    public InsertMongoCommand(MorphiumDriver d) {
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
}
