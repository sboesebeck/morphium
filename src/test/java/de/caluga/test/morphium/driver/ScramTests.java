package de.caluga.test.morphium.driver;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.client.ScramSession;
import com.ongres.scram.common.ScramMechanisms;
import com.ongres.scram.common.stringprep.StringPreparations;
import de.caluga.morphium.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static com.ongres.scram.common.stringprep.StringPreparations.NO_PREPARATION;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Tag("driver")
public class ScramTests {

    Logger log = LoggerFactory.getLogger(ScramTests.class);

    @Test
    public void testScramSHA1() throws Exception {
        ScramClient scramClient = ScramClient
                .channelBinding(ScramClient.ChannelBinding.NO)
                .stringPreparation(NO_PREPARATION)
                .selectClientMechanism(ScramMechanisms.SCRAM_SHA_1)
                .nonceSupplier(() -> "fyko+d2lbbFgONRv9qkxdawL")
                .setup();
        ScramSession scramSession = scramClient.scramSession("user");
        var msg = scramSession.clientFirstMessage();
        log.info("Message: " + msg);
        ScramSession.ServerFirstProcessor serverFirstProcessor = scramSession.receiveServerFirstMessage("r=fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHEE,s=rQ9ZY3MntBeuP3E1TDVC4w==,i=10000");
        log.info("Salt: " + serverFirstProcessor.getSalt() + ", i: " + serverFirstProcessor.getIteration());
        String passwd = "pencil";
        String user = "user";

        String pwd = user + ":mongo:" + passwd;
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(pwd.getBytes(StandardCharsets.UTF_8));
        var md5 = md.digest();
        StringBuilder hex = new StringBuilder();
        for (byte b : md5) {
            hex.append(Utils.getHex(b).toLowerCase());
        }

        log.info("Hex: " + hex.toString());
        ScramSession.ClientFinalProcessor clientFinalProcessor
                = serverFirstProcessor.clientFinalProcessor(hex.toString());
        System.out.println(clientFinalProcessor.clientFinalMessage());

        clientFinalProcessor.receiveServerFinalMessage("v=UMWeI25JD1yNYZRMpZ4VHvhZ9e0=");


    }

    @Test
    public void testScramSHA256() throws Exception {
        ScramClient scramClient = ScramClient
                .channelBinding(ScramClient.ChannelBinding.NO)
                .stringPreparation(StringPreparations.SASL_PREPARATION)
                .selectClientMechanism(ScramMechanisms.SCRAM_SHA_256)
                .nonceSupplier(() -> "rOprNGfwEbeRWgbNEkqO")
                .setup();
        ScramSession scramSession = scramClient.scramSession("user");
        var msg = scramSession.clientFirstMessage();
        log.info("Message: " + msg);
        ScramSession.ServerFirstProcessor serverFirstProcessor = scramSession.receiveServerFirstMessage("r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
        log.info("Salt: " + serverFirstProcessor.getSalt() + ", i: " + serverFirstProcessor.getIteration());
        String passwd = "pencil";
        String user = "user";

        String pwd = passwd;
//        MessageDigest md = MessageDigest.getInstance("MD5");
//        md.update(pwd.getBytes(StandardCharsets.UTF_8));
//        var md5=md.digest();
//        StringBuilder hex=new StringBuilder();
//        for (byte b:md5){
//            hex.append(Utils.getHex(b).toLowerCase());
//        }
//
//        log.info("Hex: "+hex.toString());
        ScramSession.ClientFinalProcessor clientFinalProcessor
                = serverFirstProcessor.clientFinalProcessor(pwd.toString());
        System.out.println(clientFinalProcessor.clientFinalMessage());
        //c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
        assertEquals("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=", clientFinalProcessor.clientFinalMessage());
        clientFinalProcessor.receiveServerFinalMessage("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");


    }

}
