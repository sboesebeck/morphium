package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.Map;

public interface SingleResultCommand {
    Map<String, Object> execute() throws MorphiumDriverException;

}
