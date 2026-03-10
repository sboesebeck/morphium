package de.caluga.morphium.driver.wireprotocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OpKillCursors extends WireProtocolMessage {
    private List<Long> cursorIds;

    public List<Long> getCursorIds() {
        return cursorIds;
    }

    public void setCursorIds(List<Long> cursorIds) {
        this.cursorIds = cursorIds;
    }

    public void addCursorId(Long id) {
        if (cursorIds == null) cursorIds = new ArrayList<>();
        cursorIds.add(id);
    }

    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {
        int zero = readInt(bytes, offset);
        assert (zero == 0);
        int numberOfIds = readInt(bytes, offset + 4);
        int idx = offset + 8;
        for (int i = 0; i < numberOfIds; i++) {
            addCursorId(readLong(bytes, idx));
            idx += 8;
        }
    }

    @Override
    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(0, out);
        writeInt(cursorIds.size(), out);
        for (Long id : cursorIds) {
            writeLong(id, out);
        }
        return out.toByteArray();
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_KILL_CURSORS.opCode;
    }
}
