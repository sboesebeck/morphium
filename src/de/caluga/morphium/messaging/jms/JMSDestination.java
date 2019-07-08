package de.caluga.morphium.messaging.jms;

import de.caluga.morphium.messaging.Messaging;

import javax.jms.Destination;

public abstract class JMSDestination implements Destination {

    private Messaging messaging;

    public Messaging getMessaging() {
        return messaging;
    }

    public void setMessaging(Messaging messaging) {
        this.messaging = messaging;
    }
}
