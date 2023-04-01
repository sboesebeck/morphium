package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.wire.MongoConnection;

public class DropMongoCommand extends WriteMongoCommand<DropMongoCommand> {

    public DropMongoCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "drop";
    }


}
