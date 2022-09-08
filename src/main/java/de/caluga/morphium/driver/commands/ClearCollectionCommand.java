package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.MongoConnection;

import java.util.HashMap;
import java.util.Map;

public class ClearCollectionCommand extends WriteMongoCommand<ClearCollectionCommand> {

    public ClearCollectionCommand(MongoConnection d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return null;
    }


    private DeleteMongoCommand getDelCmd() throws MorphiumDriverException {
        DeleteMongoCommand del = new DeleteMongoCommand(getConnection());
        del.addDelete(Doc.of("q", new HashMap<>(), "limit", 0));
        del.setDb(getDb());
        del.setColl(getColl());
        del.setOrdered(false);
        del.setComment(getComment());
        return del;
    }

    @Override
    public Map<String, Object> execute() throws MorphiumDriverException {
        return getDelCmd().execute();
    }

    public int doClear() throws MorphiumDriverException {
        var ret = getDelCmd().execute();
        return (Integer) ret.get("n");
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        return getDelCmd().executeAsync();
    }
}
