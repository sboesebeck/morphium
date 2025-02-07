package de.caluga.test.mongo.suite.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import de.caluga.morphium.messaging.MessageListener;
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import de.caluga.test.mongo.suite.base.MorphiumTestBase;

public class AsyncMessagingTests extends MorphiumTestBase {

    /**
     * this test will send multiple messages in parallel and check if the
     * messages send to the messagelistener callback are the correct ones
     */
    @Test
    public void multipleAsyncMessagesSending() throws InterruptedException {
        Messaging sender = new Messaging(morphium, 100, true, true, 10);
        Messaging receiver = new Messaging(morphium, 100, true, true, 10);
        int msgToSend = 10;

        try {
            sender.start();
            sender.setSenderId("Sender");
            receiver.setSenderId("receiver");
            receiver.addListenerForMessageNamed("query", (msging, msg)-> {
                log.info("Receiver got message");
                new Thread() {
                    public void run() {
                        //simulate long processing
                        try {
                            Thread.sleep(2000 + (int)(Math.random() * 5000.0));
                        } catch (InterruptedException e) {
                        }

                        Msg answer = new Msg("query", "this is the answer", "value is 42");
                        answer.setInAnswerTo(msg.getMsgId());
                        answer.setRecipient(msg.getSender());
                        msging.sendMessage(answer);
                        log.info("Sent out answer to " + msg.getSender());
                    }
                } .start();

                return null;
            });
            receiver.start();
            Thread.sleep(1000);
            final Vector<Msg> receivedMessages = new Vector();

            for (int i = 0; i < msgToSend; i++) {
                Msg query = new Msg("query", "The message", "Value");
                query.setExclusive(true);
                log.info("Sending Message");
                final int num = i;
                sender.sendAndAwaitAsync(query, 8000, (msg) -> {
                    log.info("Got message! #{}: {} value: {}", num, msg.getName(), msg.getValue());
                    assertEquals(query.getMsgId(), msg.getInAnswerTo());
                    receivedMessages.add(msg);
                });
            }

            while (receivedMessages.size() != msgToSend) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}

                log.info("Waiting for incoming message, got " + receivedMessages.size());
            }

            assertEquals(0, sender.getAsyncMessagesPending());
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }

    /**
     * simple test sending one message, asynchronously waiting for an answer
     * in this case, the timeout is 8secs, but it could be 36000
     * the timeout is just here to provide some means of cleaning up data in messaging
     */
    @Test
    public void singleAsyncMessageSending() throws InterruptedException {
        Messaging sender = new Messaging(morphium, 100, true, true, 10);
        Messaging receiver = new Messaging(morphium, 100, true, true, 10);

        try {
            sender.start();
            receiver.addListenerForMessageNamed("query", (msging, msg)-> {
                log.info("Receiver got message");
                new Thread() {
                    public void run() {
                        //simulate long processing
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                        }

                        Msg answer = new Msg("query", "this is the answer", "value is 42");
                        answer.setInAnswerTo(msg.getMsgId());
                        answer.setRecipient(msg.getSender());
                        msging.sendMessage(answer);
                        log.info("Sent out answer to " + msg.getSender());
                    }
                } .start();

                return null;
            });
            receiver.start();
            Thread.sleep(1000);
            long start = System.currentTimeMillis();
            final AtomicLong receivedAt = new  AtomicLong(0);
            Msg query = new Msg("query", "The message", "Value");
            query.setExclusive(true);
            log.info("Sending Message");
            sender.sendAndAwaitAsync(query, 8000, (msg) -> {
                log.info("Got message! {} value: {}", msg.getName(), msg.getValue());
                receivedAt.set(System.currentTimeMillis());
            });

            while (receivedAt.get() == 0l) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}

                log.info("Waiting for incoming message");
            }

            log.info("Received after {} ms", receivedAt.get() - start);
            assertEquals(0, sender.getAsyncMessagesPending());
        } finally {
            sender.terminate();
            receiver.terminate();
        }
    }


    /**
     * This test creates several receivers, who will send answers randomly
     * the sender broadcasts a message to all, and waits until timeout to
     * asynchronously process those messages
     */
    @Test
    public void broadcastAsyncMessages() throws InterruptedException {
        Messaging sender = new Messaging(morphium, 100, true, true, 10);
        Messaging receiver1 = new Messaging(morphium, 100, true, true, 10);
        Messaging receiver2 = new Messaging(morphium, 100, true, true, 10);
        Messaging receiver3 = new Messaging(morphium, 100, true, true, 10);
        Messaging receiver4 = new Messaging(morphium, 100, true, true, 10);

        try {
            sender.start();
            sender.setSenderId("Sender");
            receiver1.setSenderId("receiver1");
            receiver2.setSenderId("receiver2");
            receiver3.setSenderId("receiver3");
            receiver4.setSenderId("receiver4");
            MessageListener l = (msging, msg)-> {
                log.info("Receiver got message");
                new Thread() {
                    public void run() {
                        //simulate long processing
                        try {
                            Thread.sleep(2000 + (int)(Math.random() * 5000.0));
                        } catch (InterruptedException e) {
                        }

                        Msg answer = new Msg("query", "this is the answer", "value is 42");
                        answer.setInAnswerTo(msg.getMsgId());
                        answer.setRecipient(msg.getSender());
                        msging.sendMessage(answer);
                        log.info("{}: Sent out answer to {}", msging.getSenderId(), msg.getSender());
                    }
                } .start();

                return null;
            };
            receiver1.addListenerForMessageNamed("query", l);
            receiver1.start();
            receiver2.addListenerForMessageNamed("query", l);
            receiver2.start();
            receiver3.addListenerForMessageNamed("query", l);
            receiver3.start();
            receiver4.addListenerForMessageNamed("query", l);
            receiver4.start();
            Thread.sleep(1000);
            //just to be sure, that all is registered properly
            Msg query = new Msg("query", "The message", "Value");
            query.setExclusive(false);
            log.info("Sending Message");
            Vector<Msg> received = new Vector<Msg>();
            sender.sendAndAwaitAsync(query, 9000, (msg) -> {
                //getting back an answer from one of the receivers
                log.info("Got message from {}! {} value: {}", msg.getSender(), msg.getName(), msg.getValue());
                received.add(msg);
            });

            //waiting for answers from all receivers
            while (received.size() != 4) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}

                log.info("Waiting for incoming answers for broadcast, got {}", received.size());
            }

            log.info("Waiting to clear up...");

            while (sender.getAsyncMessagesPending() > 0) {
                Thread.sleep(250);
            }

            log.info("Cleared.");;
            assertEquals(0, sender.getAsyncMessagesPending());
            assertEquals(4, received.size()); //did still come some messages?
        } finally {
            sender.terminate();
            receiver1.terminate();
            receiver2.terminate();
            receiver3.terminate();
            receiver4.terminate();
        }
    }

}

