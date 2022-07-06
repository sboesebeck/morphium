package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.Map;

public class RenameCollectionCommand extends WriteMongoCommand<RenameCollectionCommand> {
    private String to;
    private Boolean dropTarget;

    public RenameCollectionCommand(MorphiumDriver d) {
        super(d);
    }

    public String getTo() {
        return to;
    }

    public RenameCollectionCommand setTo(String to) {
        this.to = to;
        return this;
    }

    public Boolean getDropTarget() {
        return dropTarget;
    }

    public RenameCollectionCommand setDropTarget(Boolean dropTarget) {
        this.dropTarget = dropTarget;
        return this;
    }

    @Override
    public String getCommandName() {
        return "renameCollection";
    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.put(getCommandName(), getDb() + "." + getColl());
        m.put("to", getDb() + "." + to);
        return m;
    }

    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        if (!getDriver().isConnected()) throw new RuntimeException("Not connected");
        MorphiumDriver driver = getDriver();
        if (driver == null) throw new IllegalArgumentException("you need to set the driver!");
        //noinspection unchecked

        setMetaData(Doc.of("server", driver.getHostSeed().get(0)));
        long start = System.currentTimeMillis();
        MorphiumCursor crs = driver.runCommand("admin", asMap()).getCursor();
        long dur = System.currentTimeMillis() - start;
        getMetaData().put("duration", dur);
        return crs.next();
    }
}
