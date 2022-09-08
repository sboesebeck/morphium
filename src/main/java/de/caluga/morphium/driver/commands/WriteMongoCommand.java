package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public abstract class WriteMongoCommand<T extends MongoCommand> extends MongoCommand<T> {
    private Map<String, Object> writeConcern;
    private Boolean bypassDocumentValidation;

    public WriteMongoCommand(MongoConnection d) {
        super(d);
    }

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

    public WriteMongoCommand<T> setBypassDocumentValidation(Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Map<String, Object> execute() throws MorphiumDriverException {
        if (!getConnection().isConnected()) throw new RuntimeException("Not connected");
        MongoConnection con = getConnection();

        //noinspection unchecked

        setMetaData("server", con.getConnectedTo());
        long start = System.currentTimeMillis();
        int msg = con.sendCommand(this);
        var crs = con.readSingleAnswer(msg);
        long dur = System.currentTimeMillis() - start;
        setMetaData("duration", dur);
        return crs;
    }
}
