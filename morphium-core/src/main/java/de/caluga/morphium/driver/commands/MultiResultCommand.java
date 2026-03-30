package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.List;
import java.util.Map;

/**
 * A command that returns multiple result documents.
 */
public interface MultiResultCommand {

    /** Executes the command and returns all results.
     * @return list of result documents
     * @throws MorphiumDriverException in case of error */
    List<Map<String, Object>> execute() throws MorphiumDriverException;

    /** Executes the command as an iterable cursor.
     * @param batchSize the batch size for the cursor
     * @return a morphium cursor
     * @throws MorphiumDriverException in case of error */
    MorphiumCursor executeIterable(int batchSize) throws MorphiumDriverException;

    /** Returns a map representation of this command.
     * @return map representation */
    Map<String, Object> asMap();

    /** Returns the metadata set during execution.
     * @return the metadata */
    Map<String, Object> getMetaData();

}
