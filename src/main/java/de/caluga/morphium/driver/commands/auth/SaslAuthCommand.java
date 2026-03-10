package de.caluga.morphium.driver.commands.auth;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.client.ScramSession;
import com.ongres.scram.common.ScramMechanisms;
import com.ongres.scram.common.exception.ScramInvalidServerSignatureException;
import com.ongres.scram.common.exception.ScramParseException;
import com.ongres.scram.common.exception.ScramServerErrorException;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.ongres.scram.common.stringprep.StringPreparations.NO_PREPARATION;
import static com.ongres.scram.common.stringprep.StringPreparations.SASL_PREPARATION;

public class SaslAuthCommand extends MongoCommand<SaslAuthCommand> {
    private String user;
    private String password;
    private String mechanism;


    private Logger log = LoggerFactory.getLogger(SaslAuthCommand.class);

    public SaslAuthCommand(MongoConnection c) {
        super(c);
    }

    @Override
    public String getCommandName() {
        return "saslStart";
    }

    @Override
    public int executeAsync() throws MorphiumDriverException {
        throw new RuntimeException("cannot authenticate asynchronously!");
    }

    public String getUser() {
        return user;
    }

    public SaslAuthCommand setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public SaslAuthCommand setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getMechanism() {
        return mechanism;
    }

    public SaslAuthCommand setMechanism(String mechanism) {
        this.mechanism = mechanism;
        return this;
    }


    public void execute() throws MorphiumDriverException, ScramParseException, NoSuchAlgorithmException, ScramInvalidServerSignatureException, ScramServerErrorException {
        ScramClient scramClient = null;
        if (mechanism == null) mechanism = "SCRAM-SHA-256";
        String pwd = "";
        if (mechanism.equals("SCRAM-SHA-1")) {
            scramClient = ScramClient
                    .channelBinding(ScramClient.ChannelBinding.NO)
                    .stringPreparation(NO_PREPARATION)
                    .selectClientMechanism(ScramMechanisms.SCRAM_SHA_1)
                    // .nonceSupplier(() -> "fyko+d2lbbFgONRv9qkxdawL")
                    .setup();
            pwd = user + ":mongo:" + password;
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(pwd.getBytes(StandardCharsets.UTF_8));
            var md5 = md.digest();
            StringBuilder hex = new StringBuilder();
            for (byte b : md5) {
                hex.append(Utils.getHex(b).toLowerCase());
            }
            pwd = hex.toString();
        } else if (mechanism.equals("SCRAM-SHA-256")) {
            scramClient = ScramClient
                    .channelBinding(ScramClient.ChannelBinding.NO)
                    .stringPreparation(SASL_PREPARATION)
                    .selectClientMechanism(ScramMechanisms.SCRAM_SHA_256)
                    // .nonceSupplier(() -> "fyko+d2lbbFgONRv9qkxdawL")
                    .setup();
            pwd = password;
        } else {
            throw new MorphiumDriverException("Unsupported SCRAM mechanism " + mechanism);
        }
        ScramSession scramSession = scramClient.scramSession(user);
        var msg = scramSession.clientFirstMessage();

        //Step 1: saslStart
        GenericCommand cmd = new GenericCommand(getConnection());
        cmd.setCommandName("saslStart");
        cmd.setCmdData(Doc.of("saslStart", 1, "mechanism", mechanism, "payload", msg.getBytes(StandardCharsets.UTF_8), "options", Doc.of("skipEmptyExchange", true)));
        cmd.setDb(getDb());
        var id = getConnection().sendCommand(cmd);
        var answer = getConnection().readSingleAnswer(id);
        if (!answer.containsKey("conversationId")) {
            throw new MorphiumDriverException("Error authentication: " + answer.get("errmsg"));
        }
        //answer for step one contains conversation id and payload String
        int conversationId = (Integer) answer.get("conversationId");
        String payload = new String((byte[]) answer.get("payload"));
        ScramSession.ServerFirstProcessor serverFirstProcessor = scramSession.receiveServerFirstMessage(payload);

        //Step 2: sending hashed password to mongo

        ScramSession.ClientFinalProcessor clientFinalProcessor
                = serverFirstProcessor.clientFinalProcessor(pwd);
        String s1 = clientFinalProcessor.clientFinalMessage();
        cmd = new GenericCommand(getConnection());
        cmd.setCommandName("saslContinue");
        cmd.setCmdData((Doc.of("saslContinue", 1, "conversationId", conversationId, "payload", s1.getBytes(StandardCharsets.UTF_8))));
        cmd.setDb(getDb());

        id = getConnection().sendCommand(cmd);
        answer = getConnection().readSingleAnswer(id);
        if (!answer.get("ok").equals(1.0)) {
            throw new MorphiumDriverException((String) answer.get("errmsg"));
        }
        payload = new String((byte[]) answer.get("payload"));
        clientFinalProcessor.receiveServerFinalMessage(payload);
//        log.info("Logged in");

    }
}
