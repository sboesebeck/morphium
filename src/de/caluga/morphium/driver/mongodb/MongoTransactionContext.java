package de.caluga.morphium.driver.mongodb;

import com.mongodb.client.ClientSession;
import de.caluga.morphium.driver.MorphiumTransactionContext;

/**
 * User: Stephan Bösebeck
 * Date: 03.07.18
 * Time: 22:19
 * <p>
 * TODO: Add documentation here
 */
public class MongoTransactionContext extends MorphiumTransactionContext {
    ClientSession session;

    public ClientSession getSession() {
        return session;
    }

    public void setSession(ClientSession session) {
        this.session = session;
    }
}
