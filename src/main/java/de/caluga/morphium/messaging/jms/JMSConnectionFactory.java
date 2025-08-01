package de.caluga.morphium.messaging.jms;


import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

public class JMSConnectionFactory  implements ConnectionFactory {

    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private Morphium morphium;

    public JMSConnectionFactory(Morphium morphium) {
        this.morphium = morphium;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Connection createConnection() throws JMSException {
        return new JMSConnection(morphium);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        MorphiumConfig cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());

        cfg.authSettings().setMongoLogin(userName);
        cfg.authSettings().setMongoPassword(password);
        return new JMSConnection(new Morphium(cfg));
    }

    @Override
    public JMSContext createContext() {
        return new Context(morphium);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        MorphiumConfig cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.authSettings().setMongoLogin(userName);
        cfg.authSettings().setMongoPassword(password);
        return new Context(new Morphium(cfg));
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        MorphiumConfig cfg = MorphiumConfig.fromProperties(morphium.getConfig().asProperties());
        cfg.encryptionSettings().setCredentialsEncryptionKey(morphium.getConfig().encryptionSettings().getCredentialsEncryptionKey());
        cfg.encryptionSettings().setCredentialsDecryptionKey(morphium.getConfig().encryptionSettings().getCredentialsDecryptionKey());
        cfg.authSettings().setMongoLogin(userName);
        cfg.authSettings().setMongoPassword(password);
        return new Context(new Morphium(cfg), "", sessionMode);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return new Context(morphium, "", sessionMode);

    }
}
