package de.caluga.test.morphium.driver.connection;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 30.10.15
 * Time: 23:16
 * <p>
 * TODO: Add documentation here
 */
@Disabled
public class ConnectTest extends BaseTest {
    private static String[] chars = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F",};

    private Logger log = LoggerFactory.getLogger(ConnectTest.class);

//    @Test
//    public void testConnection() throws Exception {
//
//        Socket s = new Socket("localhost", 27017);
//
//        OutputStream out = s.getOutputStream();
//        InputStream in = s.getInputStream();
//
//        //Sending a query
//
//        OpQuery q = new OpQuery();
//        q.setDb("tst");
//        q.setColl("$cmd");
//        Map<String, Object> query = new LinkedHashMap<>();
//        HashMap<String, Object> qu = new HashMap<>();
//
//        //        q.put("_id", new MongoId());
//
//        query.put("find", "test_coll");
//        query.put("limit", 123);
//        query.put("skip", 0);
//        qu.put("test", "value");
//        query.put("filter", qu);
//        Map<String, Object> sort = new HashMap<>();
//        sort.put("value", 1);
//        qu.put("sort", sort);
//        q.setDoc(query);
//
//        q.setReqId(255);
//        q.setFlags(0);
//        //        q.setLimit(10);
//        //        q.setSkip(0);
//        q.setInReplyTo(0);
//
//        //Msg...
//        //        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//        //        writeInt(1, buffer); //request-id
//        //        writeInt(0, buffer); //answer
//        //        writeInt(2004, buffer); //opcode OP_QUERY
//        //        writeInt(0, buffer); //flags
//        //        writeString("tst.test_coll", buffer);
//        //        writeInt(0, buffer); //number to skip
//        //        writeInt(10, buffer); //return
//
//
//        //        query.put("_id", new MongoId());
//        //        BsonEncoder enc = new BsonEncoder();
//        //        byte[] bytes = BsonEncoder.encodeDocument(query);
//        //        buffer.write(bytes);
//
//        //        writeInt(buffer.size() + 4, out);
//        //        out.write(buffer.toByteArray());
//        //        out.flush();
//
//
//        out.write(q.bytes());
//        out.flush();
//
//        log.info("query sent...");
//
//        byte[] inBuffer = new byte[1024];
//
//        int numRead = -1;
//
//        while (numRead == -1) {
//            numRead = in.read(inBuffer);
//        }
//        log.info("read: " + numRead + " bytes");
//
//        log.info("\n" + Utils.getHex(inBuffer, numRead));
//
//
//        OpReply reply = new OpReply();
//        reply.parse(inBuffer);
//
//        log.info("reqId (rcv) :       " + Utils.getHex(reply.getReqId()));
//        log.info("inRepl (rcv):       " + Utils.getHex(reply.getInReplyTo()));
//        log.info("flags (flags):      " + Utils.getHex(reply.getFlags()));
//        log.info("cursor (rc):" + Utils.getHex(reply.getCursorId()));
//        log.info("startFrom (rcv):    " + Utils.getHex(reply.getStartFrom()));
//        log.info("sored docs    :     " + Utils.getHex(reply.getDocuments().size()));
//
//
//    }


    @Test
    public void testConnection5() throws Exception {
        Socket s = new Socket("localhost", 27017);

        OutputStream out = s.getOutputStream();
        InputStream in = s.getInputStream();
        Doc cmd = new Doc();
        cmd.put("listCollections", 1);
        OpMsg q = new OpMsg();
        q.setMessageId(123223);


        cmd.put("limit", 1);
        cmd.put("$db", "morphium_test");
        q.setFirstDoc(cmd);
        q.setFlags(0);
        q.setResponseTo(0);
        out.write(q.bytes());
        out.flush();
        log.info("Just wrote:\n" + Utils.getHex(q.bytes()));
        WireProtocolMessage tst = WireProtocolMessage.parseFromStream(new ByteArrayInputStream(q.bytes()));


        OpMsg reply = (OpMsg) WireProtocolMessage.parseFromStream(in);
        log.info(Utils.toJsonString(reply));

        in.close();
        out.close();
        s.close();
    }

    @Test
    public void testConnectionIsMaster() throws Exception {
        Socket s = new Socket("localhost", 27017);

        OutputStream out = s.getOutputStream();
        InputStream in = s.getInputStream();

        OpMsg msg = new OpMsg();
        msg.setMessageId(12434);
        msg.setFlags(0);
        msg.setFirstDoc(Doc.of("isMaster", (Object) true).add("$db", "morphium_test"));

        out.write(msg.bytes());
        out.flush();
        log.info("Just wrote:\n" + Utils.getHex(msg.bytes()));
        WireProtocolMessage tst = WireProtocolMessage.parseFromStream(new ByteArrayInputStream(msg.bytes()));


        WireProtocolMessage reply = WireProtocolMessage.parseFromStream(in);
        log.info(Utils.toJsonString(reply));

        in.close();
        out.close();
        s.close();
    }

//
//    @Test
//    public void incomingTest() throws Exception {
//        ServerSocket ssoc = new ServerSocket(17017);
//        log.info("Opening port on 17017");
//        Socket s = ssoc.accept();
//        log.info("incoming connection");
//        InputStream in = s.getInputStream();
////        byte[] inBuffer = new byte[1024];
////
////        int numRead = -1;
////        long start=System.currentTimeMillis();
////
////        while (numRead == -1) {
////            numRead = in.read(inBuffer);
////            if (System.currentTimeMillis()-start > 1000){
////                throw new RuntimeException("Timeout - did not get answer in time");
////            }
////        }
////        log.info("read: " + numRead + " bytes");
////        log.info("\n" + Utils.getHex(inBuffer, numRead));
//        WireProtocolMessage msg = WireProtocolMessage.parseFromStream(in);
//        log.info("Message msg " + msg.getOpCode() + " " + msg.getClass().getName());
//        log.info(msg.toString());
//
//
//        //incoming.
//    }


}
