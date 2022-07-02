package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriver;

/**
 * mongo only supports conversion using the size, not max elements
 * also, this does not work on sharded cluster
 */
public class ConvertToCappedCommand extends WriteMongoCommand<ConvertToCappedCommand> {
    private int size;


    public ConvertToCappedCommand(MorphiumDriver d) {
        super(d);
    }

    public int getSize() {
        return size;
    }

    public ConvertToCappedCommand setSize(int size) {
        this.size = size;
        return this;
    }

    @Override
    public String getCommandName() {
        return "convertToCapped";
    }


}
