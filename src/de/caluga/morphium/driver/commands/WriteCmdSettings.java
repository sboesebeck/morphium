package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.Map;

public abstract class WriteCmdSettings<T extends CmdSettings> extends CmdSettings<T> {
    private Map<String, Object> writeConcern;
    private Boolean bypassDocumentValidation;

    public Map<String, Object> getWriteConcern() {
        return writeConcern;
    }

    public T setWriteConcern(Map<String, Object> writeConcern) {
        this.writeConcern = writeConcern;
        return (T) this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public WriteCmdSettings<T> setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }
}
