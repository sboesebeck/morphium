package de.caluga.morphium.driver.commands.auth;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Authenticates using the {@code MONGODB-X509} mechanism.
 *
 * <p>MongoDB Atlas X.509 authentication works as follows:
 * <ol>
 *   <li>The TLS handshake presents the client certificate to the server.</li>
 *   <li>This command sends {@code {authenticate: 1, mechanism: "MONGODB-X509", user: "<subject-dn>"}}
 *       on the {@code $external} database.</li>
 *   <li>MongoDB validates the subject DN against its registered X.509 users.</li>
 * </ol>
 *
 * <p>If {@link #setUser(String) user} is {@code null} or blank, the server derives the
 * identity from the TLS handshake automatically (supported on MongoDB 3.4+).
 * Providing the subject DN explicitly is recommended for clarity and forward compatibility.
 */
public class X509AuthCommand extends MongoCommand<X509AuthCommand> {

    private static final Logger log = LoggerFactory.getLogger(X509AuthCommand.class);

    /** The X.509 authentication database – always {@code $external} for MongoDB. */
    private static final String X509_AUTH_DB = "$external";

    /** Subject DN extracted from the client certificate (optional but recommended). */
    private String user;

    public X509AuthCommand(MongoConnection connection) {
        super(connection);
    }

    @Override
    public String getCommandName() {
        return "authenticate";
    }

    public String getUser() {
        return user;
    }

    /**
     * Sets the subject DN of the client certificate.
     * Should be in RFC 2253 format, e.g. {@code "CN=myUser,O=myOrg,C=DE"}.
     * Pass {@code null} to let the server derive it from the TLS session.
     */
    public X509AuthCommand setUser(String user) {
        this.user = user;
        return this;
    }

    /**
     * Executes the X.509 authenticate command.
     *
     * @throws MorphiumDriverException if authentication fails
     */
    public void execute() throws MorphiumDriverException {
        Doc cmdData = Doc.of(
                "authenticate", 1,
                "mechanism", "MONGODB-X509"
        );

        if (user != null && !user.isBlank()) {
            cmdData.put("user", user);
        }

        GenericCommand cmd = new GenericCommand(getConnection());
        cmd.setCommandName("authenticate");
        cmd.setCmdData(cmdData);
        cmd.setDb(X509_AUTH_DB);

        int id = getConnection().sendCommand(cmd);
        Map<String, Object> response = getConnection().readSingleAnswer(id);

        Object ok = response.get("ok");
        if (ok == null || (ok instanceof Number && ((Number) ok).doubleValue() != 1.0)) {
            throw new MorphiumDriverException(
                    "X.509 authentication failed: " + response.get("errmsg")
                            + " (code: " + response.get("code") + ")");
        }

        log.debug("X.509 authentication successful for user '{}'", user != null ? user : "<from-certificate>");
    }
}
