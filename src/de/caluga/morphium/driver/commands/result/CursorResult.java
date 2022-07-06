package de.caluga.morphium.driver.commands.result;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.commands.result.RunCommandResult;

public class CursorResult extends RunCommandResult<CursorResult> {
    private MorphiumCursor cursor;

    public MorphiumCursor getCursor() {
        return cursor;
    }

    public CursorResult setCursor(MorphiumCursor cursor) {
        this.cursor = cursor;
        return this;
    }

}
