package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.ArrayList;
import java.util.List;

public class CreateIndexesCmd extends WriteMongoCommand<CreateIndexesCmd> {
    private List<Doc> indexes;
    private String commitQuorum;

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
