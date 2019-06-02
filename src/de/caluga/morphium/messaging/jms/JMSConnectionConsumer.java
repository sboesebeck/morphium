package de.caluga.morphium.messaging.jms;

import javax.jms.ConnectionConsumer;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;

public class JMSConnectionConsumer implements ConnectionConsumer {
    @Override
    public ServerSessionPool getServerSessionPool() throws JMSException {
        return null;
    }

    @Override
    public void close() throws JMSException {

    }
}
