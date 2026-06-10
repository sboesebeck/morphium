package de.caluga.test.morphium;

import de.caluga.morphium.driver.DnsSrvResolver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class DnsSrvResolverTest {

    // ── encodeDnsName ─────────────────────────────────────────────────────────

    @Test
    void encodeDnsName_simpleHost() {
        // "foo.bar.com" → \x03foo\x03bar\x03com\x00
        byte[] encoded = DnsSrvResolver.encodeDnsName("foo.bar.com");
        byte[] expected = {
            3, 'f', 'o', 'o',
            3, 'b', 'a', 'r',
            3, 'c', 'o', 'm',
            0
        };
        assertArrayEquals(expected, encoded);
    }

    // ── buildDnsQuery ─────────────────────────────────────────────────────────

    @Test
    void buildDnsQuery_hasCorrectTypeAndClass() {
        byte[] query = DnsSrvResolver.buildDnsQuery("_mongodb._tcp.cluster.example.com", 33);

        // QDCOUNT must be 1 (bytes 4-5)
        assertEquals(0, query[4]);
        assertEquals(1, query[5]);

        // RD bit must be set (byte 2)
        assertTrue((query[2] & 0x01) != 0, "RD bit should be set");

        // After the 12-byte header + encoded name, find QTYPE and QCLASS
        byte[] name = DnsSrvResolver.encodeDnsName("_mongodb._tcp.cluster.example.com");
        int off = 12 + name.length;
        int qtype  = DnsSrvResolver.dnsShort(query, off);
        int qclass = DnsSrvResolver.dnsShort(query, off + 2);

        assertEquals(33, qtype,  "QTYPE must be 33 (SRV)");
        assertEquals(1,  qclass, "QCLASS must be 1 (IN)");
    }

    // ── parseSrvRecords ───────────────────────────────────────────────────────

    /**
     * Builds a minimal, self-contained SRV DNS response for one record:
     *   host.example.com  port 27017
     *
     * Layout (no compression, one question, one answer):
     *
     *   Header  (12 bytes)
     *   Question: _mongodb._tcp.cluster → QTYPE=33, QCLASS=1
     *   Answer:   NAME=\xC0\x0C  TYPE=33  CLASS=1  TTL=300  RDLEN=…
     *             RDATA: priority(2) weight(2) port(2) target(encoded)
     */
    private static byte[] buildSingleSrvResponse(String qname, String targetHost, int port) {
        byte[] qnameEnc  = DnsSrvResolver.encodeDnsName(qname);
        byte[] targetEnc = DnsSrvResolver.encodeDnsName(targetHost);

        int rdLen = 6 + targetEnc.length; // priority(2)+weight(2)+port(2)+target

        // Header
        byte[] hdr = {
            0x00, 0x01,  // ID
            (byte)0x81, 0x00,  // QR=1 AA=0 TC=0 RD=1 RA=0 RCODE=0
            0x00, 0x01,  // QDCOUNT=1
            0x00, 0x01,  // ANCOUNT=1
            0x00, 0x00,  // NSCOUNT=0
            0x00, 0x00   // ARCOUNT=0
        };

        // Question section
        byte[] question = new byte[qnameEnc.length + 4];
        System.arraycopy(qnameEnc, 0, question, 0, qnameEnc.length);
        question[qnameEnc.length]     = 0x00; question[qnameEnc.length + 1] = 0x21; // QTYPE=33
        question[qnameEnc.length + 2] = 0x00; question[qnameEnc.length + 3] = 0x01; // QCLASS=1

        // Answer section: NAME(2) + TYPE(2) + CLASS(2) + TTL(4) + RDLENGTH(2) + RDATA(rdLen)
        // NAME: compression pointer to offset 12 (start of question name)
        byte[] answer = new byte[12 + rdLen];
        answer[0] = (byte)0xC0; answer[1] = 0x0C;  // NAME: pointer to offset 12
        answer[2] = 0x00; answer[3] = 0x21;         // TYPE=33 (SRV)
        answer[4] = 0x00; answer[5] = 0x01;         // CLASS=1 (IN)
        answer[6] = 0x00; answer[7] = 0x00;         // TTL high
        answer[8] = 0x00; answer[9] = (byte)0x2C;   // TTL low (= 300 total? no, TTL is 4 bytes: 0x0000002C=44)
        answer[10] = (byte)(rdLen >> 8);
        answer[11] = (byte)(rdLen & 0xFF);           // RDLENGTH
        // RDATA: priority=0, weight=0, port=port, target
        answer[12] = 0x00; answer[13] = 0x00;        // priority
        answer[14] = 0x00; answer[15] = 0x00;        // weight
        answer[16] = (byte)(port >> 8);
        answer[17] = (byte)(port & 0xFF);             // port
        System.arraycopy(targetEnc, 0, answer, 18, targetEnc.length);

        byte[] response = new byte[hdr.length + question.length + answer.length];
        System.arraycopy(hdr,      0, response, 0,                                hdr.length);
        System.arraycopy(question, 0, response, hdr.length,                       question.length);
        System.arraycopy(answer,   0, response, hdr.length + question.length,     answer.length);
        return response;
    }

    @Test
    void parseSrvRecords_singleRecord() throws Exception {
        byte[] response = buildSingleSrvResponse(
            "_mongodb._tcp.cluster.example.com",
            "host.example.com",
            27017
        );

        List<String[]> records = DnsSrvResolver.parseSrvRecords(response);

        assertEquals(1, records.size());
        assertEquals("host.example.com", records.get(0)[0]);
        assertEquals("27017",            records.get(0)[1]);
    }

    @Test
    void parseSrvRecords_emptyAnswer() throws Exception {
        // Minimal valid response with ANCOUNT=0
        byte[] response = {
            0x00, 0x01,              // ID
            (byte)0x81, 0x00,        // flags: QR=1 RCODE=0
            0x00, 0x00,              // QDCOUNT=0
            0x00, 0x00,              // ANCOUNT=0
            0x00, 0x00,              // NSCOUNT=0
            0x00, 0x00               // ARCOUNT=0
        };

        List<String[]> records = DnsSrvResolver.parseSrvRecords(response);

        assertNotNull(records);
        assertTrue(records.isEmpty(), "Empty answer section should yield empty list");
    }

    @Test
    void parseSrvRecords_tooShortThrows() {
        byte[] tooShort = new byte[10];
        assertThrows(Exception.class, () -> DnsSrvResolver.parseSrvRecords(tooShort));
    }

    // ── systemDnsServers fallback behaviour (issue #170) ──────────────────────

    private static final String NAMESERVER_PROP = "sun.net.spi.nameservice.nameservers";

    private static boolean containsAddr(List<InetAddress> servers, String addr) {
        return servers.stream().anyMatch(a -> a.getHostAddress().equals(addr));
    }

    @Test
    void systemDnsServers_doesNotAddPublicFallback_whenSystemServersPresent() throws Exception {
        String savedProp = System.getProperty(NAMESERVER_PROP);
        File resolvConf = File.createTempFile("resolv", ".conf");
        try {
            System.clearProperty(NAMESERVER_PROP);
            Files.writeString(resolvConf.toPath(), "nameserver 10.123.45.67\n");

            List<InetAddress> servers = DnsSrvResolver.systemDnsServers(resolvConf);

            assertTrue(containsAddr(servers, "10.123.45.67"), "configured system name-server should be present");
            assertFalse(containsAddr(servers, "8.8.8.8"), "public DNS fallback must not be added when system servers exist (issue #170)");
            assertFalse(containsAddr(servers, "1.1.1.1"), "public DNS fallback must not be added when system servers exist (issue #170)");
        } finally {
            //noinspection ResultOfMethodCallIgnored
            resolvConf.delete();
            restoreProp(savedProp);
        }
    }

    @Test
    void systemDnsServers_addsPublicFallback_whenNoSystemServers() throws Exception {
        String savedProp = System.getProperty(NAMESERVER_PROP);
        File missing = new File(File.createTempFile("resolv", ".conf").getAbsolutePath() + ".gone");
        try {
            System.clearProperty(NAMESERVER_PROP);
            assertFalse(missing.exists(), "test precondition: resolv.conf must not exist");

            List<InetAddress> servers = DnsSrvResolver.systemDnsServers(missing);

            assertTrue(containsAddr(servers, "8.8.8.8"), "public DNS fallback should be used when no system servers are found");
            assertTrue(containsAddr(servers, "1.1.1.1"), "public DNS fallback should be used when no system servers are found");
        } finally {
            restoreProp(savedProp);
        }
    }

    @Test
    void systemDnsServers_neverEmpty() {
        // The public entry point must always yield at least one resolver to query.
        assertFalse(DnsSrvResolver.systemDnsServers().isEmpty(), "must always return at least one DNS server to query");
    }

    private static void restoreProp(String savedProp) {
        if (savedProp != null) {
            System.setProperty(NAMESERVER_PROP, savedProp);
        } else {
            System.clearProperty(NAMESERVER_PROP);
        }
    }
}
