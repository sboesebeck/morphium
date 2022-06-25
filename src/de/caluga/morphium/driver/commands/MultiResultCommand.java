package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.List;
import java.util.Map;

public interface MultiResultCommand {

    List<Map<String, Object>> execute() throws MorphiumDriverException;

    MorphiumCursor executeIterable() throws MorphiumDriverException;

}
