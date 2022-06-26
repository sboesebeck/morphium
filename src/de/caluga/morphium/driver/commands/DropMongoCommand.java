package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriver;

public class DropMongoCommand extends WriteMongoCommand<DropMongoCommand> {

    public DropMongoCommand(MorphiumDriver d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "drop";
    }


}
