package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.sync.NetworkCallHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CreateIndexesCmd extends WriteMongoCommand<CreateIndexesCmd> {
    private List<Doc> indexes;
    private String commitQuorum;

    public CreateIndexesCmd(MorphiumDriver d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "createIndexes";
    }

    public List<Doc> getIndexes() {
        return indexes;
    }

    public CreateIndexesCmd setIndexes(List<Doc> indexes) {
        this.indexes = indexes;
        return this;
    }

    public String getCommitQuorum() {
        return commitQuorum;
    }

    public CreateIndexesCmd setCommitQuorum(String commitQuorum) {
        this.commitQuorum = commitQuorum;
        return this;
    }


    public CreateIndexesCmd addIndex(IndexDescription idx) {
        if (indexes == null) indexes = new ArrayList<>();
        indexes.add(idx.asMap());
        return this;
    }

}
