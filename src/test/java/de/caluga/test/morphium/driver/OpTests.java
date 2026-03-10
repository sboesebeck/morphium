package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.*;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("core")
public class OpTests {

    @Test
    public void TestOpCompressed() {
        OpCompressed c = new OpCompressed();
    }

    @Test
    public void TestOpGetMore() throws Exception {
        OpGetMore op = new OpGetMore();
        op.setMessageId(123);
        op.setResponseTo(222);
        op.setCursorId(12345);
        op.setNumberToReturn(1);
        op.setFullCollectionName("test.collection");

        byte[] data = op.bytes();
        assertThat(data.length).isGreaterThan(0);

        OpGetMore op2 = (OpGetMore) WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assertEquals(op.getCursorId(), op2.getCursorId());
        assertEquals(op.getFullCollectionName(), op2.getFullCollectionName());
        assertEquals(op.getResponseTo(), op2.getResponseTo());
        assertEquals(op.getMessageId(), op2.getMessageId());
    }


    @Test
    public void TestOpInsert() throws Exception {
        OpInsert op = new OpInsert();
        op.setFullConnectionName("test.collection");
        op.setFlags(OpInsert.CONTINUE_ON_ERROR_FLAG);
        op.setMessageId(42);
        op.setResponseTo(12345);
        op.addDocument(Doc.of("test", (Object) "value").add("test2", 123));
        op.addDocument(Doc.of("test", (Object) "value2").add("test2", 124));


        byte[] data = op.bytes();
        assertThat(data).isNotEmpty();

        OpInsert parsed = (OpInsert) WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assertThat(parsed.getDocuments()).hasSize(2);
        assertEquals(op.getFlags(), parsed.getFlags());
        assertEquals(op.getMessageId(), parsed.getMessageId());
        assertEquals(op.getResponseTo(), parsed.getResponseTo());
        assertThat(parsed.getDocuments().get(0)).containsKey("test");
        assertThat(parsed.getDocuments().get(1)).containsKey("test");
        assertEquals("value2", parsed.getDocuments().get(1).get("test"));

    }


    @Test
    public void TestOpDelete() throws Exception {
        OpDelete op = new OpDelete();
        op.setFullCollectionName("test.collection");
        op.setFlags(OpInsert.CONTINUE_ON_ERROR_FLAG);
        op.setMessageId(42);
        op.setResponseTo(12345);
        op.setSelector(Doc.of("test", (Object) "value").add("test2", 123));

        byte[] data = op.bytes();
        assertThat(data).isNotEmpty();

        OpDelete parsed = (OpDelete) WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assertEquals(op.getFlags(), parsed.getFlags());
        assertEquals(op.getMessageId(), parsed.getMessageId());
        assertEquals(op.getResponseTo(), parsed.getResponseTo());
        assertThat(parsed.getSelector()).containsKey("test");
        assertThat(parsed.getSelector()).containsKey("test2");

    }

    @Test
    public void TestOpKillCursors() throws Exception {
        OpKillCursors op = new OpKillCursors();
        op.setMessageId(42);
        op.setResponseTo(12345);
        op.addCursorId(42L);
        op.addCursorId(123L);
        op.addCursorId(System.currentTimeMillis());

        byte[] data = op.bytes();
        assertThat(data).isNotEmpty();

        OpKillCursors parsed = (OpKillCursors) WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assertEquals(op.getMessageId(), parsed.getMessageId());
        assertEquals(op.getResponseTo(), parsed.getResponseTo());
        assertEquals(op.getCursorIds().size(), parsed.getCursorIds().size());
        assertEquals(op.getCursorIds(), parsed.getCursorIds());

    }

    @Test
    public void TestOpReply() throws Exception {
        OpReply op = new OpReply();
        op.setMessageId(42);
        op.setResponseTo(12345);
        op.setCursorId(12345);
        op.setFlags(OpReply.AWAIT_CAPABLE_FLAG | OpReply.QUERY_FAILURE_FLAG);
        op.addDocument(Doc.of("test", (Object) 123).add("value", "val"));
        op.addDocument(Doc.of("test", (Object) 42).add("value", "val2"));
        op.setNumReturned(2);
        op.setStartFrom(123);

        byte[] data = op.bytes();
        assertThat(data).isNotEmpty();

        OpReply parsed = (OpReply) WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assertEquals(op.getMessageId(), parsed.getMessageId());
        assertEquals(op.getResponseTo(), parsed.getResponseTo());
        assertEquals(op.getCursorId(), parsed.getCursorId());
        assertEquals(op.getFlags(), parsed.getFlags());
        assertEquals(op.getDocuments().size(), parsed.getDocuments().size());
        assertEquals(op.getNumReturned(), parsed.getNumReturned());
        assertEquals(parsed.getDocuments().get(0).get("test"), parsed.getDocuments().get(0).get("test"));

    }


    @Test
    public void TestOpUpdate() throws Exception {
        OpUpdate op = new OpUpdate();
        op.setMessageId(42);
        op.setResponseTo(12345);
        op.setFlags(OpUpdate.UPSERT_FLAG);
        op.setFullCollectionName("testdb.collection");
        op.setSelector(Doc.of("test", 123));
        op.setUpdate(Doc.of("$set", Doc.of("test", 1222)));

        byte[] data = op.bytes();
        assertThat(data).isNotEmpty();

        OpUpdate parsed = (OpUpdate) WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assertEquals(op.getMessageId(), parsed.getMessageId());
        assertEquals(op.getResponseTo(), parsed.getResponseTo());
        assertEquals(op.getFlags(), parsed.getFlags());
        assertEquals(op.getUpdate(), parsed.getUpdate());
        assertEquals(op.getSelector(), parsed.getSelector());
        assertEquals(op.getFullCollectionName(), parsed.getFullCollectionName());

    }


    @Test
    public void TestOpMsg() throws Exception {
        OpMsg op = new OpMsg();
        op.setMessageId(42);
        op.setResponseTo(12345);
        op.setFlags(OpMsg.CHECKSUM_PRESENT);
        op.setFirstDoc(Doc.of("$cmd", Doc.of("hello", 1)));
        op.setFirstDoc(Doc.of("$find", Doc.of("test", 42)));


        byte[] data = op.bytes();
        assertThat(data).isNotEmpty();

        OpMsg parsed = (OpMsg) WireProtocolMessage.parseFromStream(new ByteArrayInputStream(data));
        assertEquals(op.getMessageId(), parsed.getMessageId());
        assertEquals(op.getResponseTo(), parsed.getResponseTo());
        assertEquals(op.getFlags(), parsed.getFlags());
        assertThat(parsed.getFirstDoc()).hasSize(op.getFirstDoc().size());

    }


}
