package de.caluga.morphium.messaging.jms;


import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JMSConnectionFactory  implements ConnectionFactory {


    static {
        try {
            InitialContext ctx=new InitialContext();
            ctx.rebind("morphiumMessagingConnectionFactory",new JMSConnectionFactory());
        } catch (NamingException e) {
            e.printStackTrace();
        }
    }
    @Override
    public Connection createConnection() throws JMSException {
        return null;
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return null;
    }

    @Override
    public JMSContext createContext() {
        return null;
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        return null;
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        return null;
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return null;
    }
}
