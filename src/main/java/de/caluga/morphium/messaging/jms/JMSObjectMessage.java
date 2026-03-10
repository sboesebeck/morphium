package de.caluga.morphium.messaging.jms;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.Serializable;

public class JMSObjectMessage extends JMSMessage implements ObjectMessage {

    private Serializable data;

    @SuppressWarnings("RedundantThrows")
    @Override
    public Serializable getObject() throws JMSException {
        return data;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void setObject(Serializable object) throws JMSException {
        data = object;
    }
}
