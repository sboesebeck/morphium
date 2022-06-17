package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.Doc;

import java.util.List;

public class StoreCmdSettings extends WriteCmdSettings<StoreCmdSettings> {
    private List<Doc> docs;

    public List<Doc> getDocs() {
        return docs;
    }

    public StoreCmdSettings setDocs(List<Doc> docs) {
        this.docs = docs;
        return this;
    }
}
