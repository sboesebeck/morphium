package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriver;

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
}
