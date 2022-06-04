package de.caluga.test.morphium.driver;

import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wireprotocol.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(op2.getCursorId()).isEqualTo(op.getCursorId());
        assertThat(op2.getFullCollectionName()).isEqualTo(op.getFullCollectionName());
        assertThat(op2.getResponseTo()).isEqualTo(op.getResponseTo());
        assertThat(op2.getMessageId()).isEqualTo(op.getMessageId());
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
        assertThat(parsed.getFlags()).isEqualTo(op.getFlags());
        assertThat(parsed.getMessageId()).isEqualTo(op.getMessageId());
        assertThat(parsed.getResponseTo()).isEqualTo(op.getResponseTo());
        assertThat(parsed.getDocuments().get(0)).containsKey("test");
        assertThat(parsed.getDocuments().get(1)).containsKey("test");
        assertThat(parsed.getDocuments().get(1).get("test")).isEqualTo("value2");

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
        assertThat(parsed.getFlags()).isEqualTo(op.getFlags());
        assertThat(parsed.getMessageId()).isEqualTo(op.getMessageId());
        assertThat(parsed.getResponseTo()).isEqualTo(op.getResponseTo());
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
        assertThat(parsed.getMessageId()).isEqualTo(op.getMessageId());
        assertThat(parsed.getResponseTo()).isEqualTo(op.getResponseTo());
        assertThat(parsed.getCursorIds().size()).isEqualTo(op.getCursorIds().size());
        assertThat(parsed.getCursorIds()).isEqualTo(op.getCursorIds());

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
        assertThat(parsed.getMessageId()).isEqualTo(op.getMessageId());
        assertThat(parsed.getResponseTo()).isEqualTo(op.getResponseTo());
        assertThat(parsed.getCursorId()).isEqualTo(op.getCursorId());
        assertThat(parsed.getFlags()).isEqualTo(op.getFlags());
        assertThat(parsed.getDocuments().size()).isEqualTo(op.getDocuments().size());
        assertThat(parsed.getNumReturned()).isEqualTo(op.getNumReturned());
        assertThat(parsed.getDocuments().get(0).get("test")).isEqualTo(parsed.getDocuments().get(0).get("test"));

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
        assertThat(parsed.getMessageId()).isEqualTo(op.getMessageId());
        assertThat(parsed.getResponseTo()).isEqualTo(op.getResponseTo());
        assertThat(parsed.getFlags()).isEqualTo(op.getFlags());
        assertThat(parsed.getUpdate()).isEqualTo(op.getUpdate());
        assertThat(parsed.getSelector()).isEqualTo(op.getSelector());
        assertThat(parsed.getFullCollectionName()).isEqualTo(op.getFullCollectionName());

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
        assertThat(parsed.getMessageId()).isEqualTo(op.getMessageId());
        assertThat(parsed.getResponseTo()).isEqualTo(op.getResponseTo());
        assertThat(parsed.getFlags()).isEqualTo(op.getFlags());
        assertThat(parsed.getFirstDoc()).hasSize(op.getFirstDoc().size());

    }


}
