package de.caluga.morphium.driver.wireprotocol;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OpInsert extends WireProtocolMessage {
    public static final int CONTINUE_ON_ERROR_FLAG = 1;

    private int flags;
    private String fullConnectionName;
    private List<Doc> documents;


    public void addDocument(Doc doc) {
        if (documents == null) {
            documents = new ArrayList<>();
        }
        documents.add(doc);
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public String getFullConnectionName() {
        return fullConnectionName;
    }

    public void setFullConnectionName(String fullConnectionName) {
        this.fullConnectionName = fullConnectionName;
    }

    public List<Doc> getDocuments() {
        return documents;
    }

    public void setDocuments(List<Doc> documents) {
        this.documents = documents;
    }

    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {
        flags = readInt(bytes, offset);
        fullConnectionName = readString(bytes, offset + 4);
        int strLen = strLen(bytes, offset + 4);
        documents = new ArrayList<>();
        int idx = offset + 4 + strLen;
        while (idx < bytes.length) {
            Doc doc = new Doc();
            int len = BsonDecoder.decodeDocumentIn(doc, bytes, idx);
            documents.add(doc);
            idx += len;
        }
    }

    @Override
    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(flags, out);
        writeString(fullConnectionName, out);
        for (Doc doc : documents) {
            out.write(BsonEncoder.encodeDocument(doc));
        }
        return out.toByteArray();
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_INSERT.opCode;
    }
}
