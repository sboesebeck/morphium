package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.Map;

/**
 * a command that returns a single result
 */
public interface SingleResultCommand {
    /**
     * Executes this command and returns the result.
     * @return the execution result
     * @throws MorphiumDriverException in case of error
     */
    Map<String, Object> execute() throws MorphiumDriverException;

    /**
     * Returns the metadata set during execution.
     * @return the metadata
     */
    Map<String, Object> getMetaData();

    /**
     * Returns a map representation of this command.
     * @return map representation
     */
    Map<String, Object> asMap();


}
