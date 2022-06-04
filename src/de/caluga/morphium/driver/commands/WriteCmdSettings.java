package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

public abstract class WriteCmdSettings<T extends CmdSettings> extends CmdSettings<T> {
    private Doc writeConcern;
    private Boolean bypassDocumentValidation;

    public Doc getWriteConcern() {
        return writeConcern;
    }

    public T setWriteConcern(Doc writeConcern) {
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
