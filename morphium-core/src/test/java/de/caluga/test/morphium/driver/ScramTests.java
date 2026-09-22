package de.caluga.test.morphium.driver;

import com.ongres.scram.client.ChannelBindingPolicy;
import com.ongres.scram.client.ScramClient;
import com.ongres.scram.common.StringPreparation;
import de.caluga.morphium.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Tag("driver")
public class ScramTests {

    Logger log = LoggerFactory.getLogger(ScramTests.class);

    @Test
    public void testScramSHA1() throws Exception {
        String user = "user";
        String passwd = "pencil";

        String pwd = user + ":mongo:" + passwd;
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(pwd.getBytes(StandardCharsets.UTF_8));
        var md5 = md.digest();
        StringBuilder hex = new StringBuilder();
        for (byte b : md5) {
            hex.append(Utils.getHex(b).toLowerCase());
        }

        ScramClient scramClient = ScramClient.builder()
                .advertisedMechanisms(List.of("SCRAM-SHA-1"))
                .username(user)
                .password(hex.toString().toCharArray())
                .channelBindingPolicy(ChannelBindingPolicy.DISABLE)
                .stringPreparation(StringPreparation.NO_PREPARATION)
                .nonceSupplier(() -> "fyko+d2lbbFgONRv9qkxdawL")
                .build();
        var msg = scramClient.clientFirstMessage();
        log.info("Message: " + msg);
        var serverFirst = scramClient.serverFirstMessage("r=fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHEE,s=rQ9ZY3MntBeuP3E1TDVC4w==,i=10000");
        log.info("Salt: " + serverFirst.getSalt() + ", i: " + serverFirst.getIterationCount());

        System.out.println(scramClient.clientFinalMessage());

        scramClient.serverFinalMessage("v=UMWeI25JD1yNYZRMpZ4VHvhZ9e0=");


    }

    @Test
    public void testScramSHA256() throws Exception {
        ScramClient scramClient = ScramClient.builder()
                .advertisedMechanisms(List.of("SCRAM-SHA-256"))
                .username("user")
                .password("pencil".toCharArray())
                .channelBindingPolicy(ChannelBindingPolicy.DISABLE)
                .stringPreparation(StringPreparation.SASL_PREPARATION)
                .nonceSupplier(() -> "rOprNGfwEbeRWgbNEkqO")
                .build();
        var msg = scramClient.clientFirstMessage();
        log.info("Message: " + msg);
        var serverFirst = scramClient.serverFirstMessage("r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
        log.info("Salt: " + serverFirst.getSalt() + ", i: " + serverFirst.getIterationCount());
        //c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
        assertEquals("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=", scramClient.clientFinalMessage().toString());
        scramClient.serverFinalMessage("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");


    }

}
