package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriver;

import java.util.ArrayList;
import java.util.List;

public class KillCursorsCommand extends WriteMongoCommand<KillCursorsCommand> {
    private List<Long> cursorIds;

    public KillCursorsCommand(MorphiumDriver d) {
        super(d);
    }

    public KillCursorsCommand setCursorIds(List<Long> ids) {
        cursorIds = ids;
        return this;
    }

    public KillCursorsCommand setCursorIds(long... ids) {
        cursorIds = new ArrayList<>();
        for (long l : ids) {
            if (l != 0) {
                cursorIds.add(l);
            }
        }
        return this;
    }

    @Override
    public String getCommandName() {
        return "killCursors";
    }
}
