package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.ArrayList;
import java.util.List;

public class KillCursorsCommand extends WriteMongoCommand<KillCursorsCommand> {
    private List<Long> cursors;

    public KillCursorsCommand(MongoConnection d) {
        super(d);
    }

    public KillCursorsCommand setCursorIds(long... ids) {
        cursors = new ArrayList<>();
        for (long l : ids) {
            if (l != 0) {
                cursors.add(l);
            }
        }
        return this;
    }

    public List<Long> getCursors() {
        return cursors;
    }

    public KillCursorsCommand setCursors(List<Long> ids) {
        cursors = ids;
        return this;
    }

    @Override
    public String getCommandName() {
        return "killCursors";
    }
}
