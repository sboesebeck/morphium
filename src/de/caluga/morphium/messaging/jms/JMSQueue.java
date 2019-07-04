package de.caluga.morphium.messaging.jms;

import javax.jms.JMSException;
import javax.jms.Queue;

public class JMSQueue implements Queue {
    @Override
    public String getQueueName() throws JMSException {
        return null;
    }
}
