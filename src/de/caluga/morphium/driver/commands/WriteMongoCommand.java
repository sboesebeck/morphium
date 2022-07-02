package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.Map;

public abstract class WriteMongoCommand<T extends MongoCommand> extends MongoCommand<T> {
    private Map<String, Object> writeConcern;
    private Boolean bypassDocumentValidation;

    public WriteMongoCommand(MorphiumDriver d) {
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
        if (!getDriver().isConnected()) throw new RuntimeException("Not connected");
        MorphiumDriver driver = getDriver();

        //noinspection unchecked

        setMetaData(Doc.of("server", driver.getHostSeed()[0]));
        long start = System.currentTimeMillis();
        MorphiumCursor crs = driver.runCommand(getDb(), asMap());
        long dur = System.currentTimeMillis() - start;
        getMetaData().put("duration", dur);
        return crs.next();
    }
}
