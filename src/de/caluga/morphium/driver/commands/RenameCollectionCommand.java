package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.Map;

public class RenameCollectionCommand extends AdminMongoCommand<RenameCollectionCommand> {
    private String to;
    private Boolean dropTarget;

    public RenameCollectionCommand(MongoConnection d) {
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

}
