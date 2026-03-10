package de.caluga.morphium.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for resolving MongoDB SRV records via pure-Java DNS (no JNDI).
 * Works in any JVM environment including Quarkus native, Android, and Windows.
 */
public final class DnsSrvResolver {

    private static final Logger log = LoggerFactory.getLogger(DnsSrvResolver.class);
    private static final int DNS_SRV_PORT       = 53;
    private static final int DNS_SRV_TIMEOUT_MS = 5_000;

    private DnsSrvResolver() {}

    /**
     * Resolves the given DNS SRV name and returns a list of {@code "host:port"} strings.
     * Tries each system DNS server in turn; returns on the first non-empty result.
     *
     * @param srvName the fully-qualified SRV name, e.g. {@code _mongodb._tcp.cluster.mongodb.net}
     * @return list of resolved host:port strings (never {@code null})
     * @throws Exception if no DNS servers could be found or all queries failed
     */
    public static List<String> resolve(String srvName) throws Exception {
        List<InetAddress> servers = systemDnsServers();
        if (servers.isEmpty()) {
            throw new Exception("No DNS name-servers found on this host");
        }
        Exception lastEx = null;
        for (InetAddress dns : servers) {
            try {
                List<String[]> records = srvQuery(dns, srvName);
                if (!records.isEmpty()) {
                    List<String> result = new ArrayList<>(records.size());
                    for (String[] hp : records) {
                        result.add(hp[0] + ":" + hp[1]);
                    }
                    return result;
                }
                log.debug("DNS server {} returned no SRV records for '{}'", dns, srvName);
            } catch (Exception ex) {
                lastEx = ex;
                log.debug("DNS server {} failed for '{}': {}", dns, srvName, ex.getMessage());
            }
        }
        if (lastEx != null) throw lastEx;
        return Collections.emptyList();
    }

    /** Collects name-server addresses from JVM properties, /etc/resolv.conf (non-Windows), and public fallbacks. */
    public static List<InetAddress> systemDnsServers() {
        List<InetAddress> servers = new ArrayList<>();

        String prop = System.getProperty("sun.net.spi.nameservice.nameservers");
        if (prop != null) {
            for (String s : prop.split(",")) {
                try { servers.add(InetAddress.getByName(s.trim())); } catch (Exception ignored) {}
            }
        }

        String os = System.getProperty("os.name", "").toLowerCase();
        if (!os.contains("win")) {
            File resolvConf = new File("/etc/resolv.conf");
            if (resolvConf.exists()) {
                try (BufferedReader br = new BufferedReader(new FileReader(resolvConf))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.startsWith("nameserver ")) {
                            String addr = line.substring("nameserver ".length()).trim();
                            try { servers.add(InetAddress.getByName(addr)); } catch (Exception ignored) {}
                        }
                    }
                } catch (Exception ignored) {}
            }
        }

        // Always add public fallbacks so Windows (and restricted environments) work reliably
        try { servers.add(InetAddress.getByName("8.8.8.8")); } catch (Exception ignored) {}
        try { servers.add(InetAddress.getByName("1.1.1.1")); } catch (Exception ignored) {}

        return servers;
    }

    /** Sends a DNS SRV query over UDP; retries over TCP if the response is truncated. */
    private static List<String[]> srvQuery(InetAddress dns, String srvName) throws Exception {
        byte[] query    = buildDnsQuery(srvName, 33 /* SRV */);
        byte[] response = dnsOverUdp(dns, query);
        // TC bit (byte 2, bit 1) set → response was truncated, retry over TCP
        if ((response[2] & 0x02) != 0) {
            log.debug("DNS UDP response truncated, retrying over TCP");
            response = dnsOverTcp(dns, query);
        }
        return parseSrvRecords(response);
    }

    private static byte[] dnsOverUdp(InetAddress dns, byte[] query) throws Exception {
        try (DatagramSocket sock = new DatagramSocket()) {
            sock.setSoTimeout(DNS_SRV_TIMEOUT_MS);
            sock.send(new DatagramPacket(query, query.length, dns, DNS_SRV_PORT));
            byte[] buf   = new byte[4096];
            DatagramPacket reply = new DatagramPacket(buf, buf.length);
            sock.receive(reply);
            return Arrays.copyOf(buf, reply.getLength());
        }
    }

    private static byte[] dnsOverTcp(InetAddress dns, byte[] query) throws Exception {
        try (java.net.Socket sock = new java.net.Socket()) {
            sock.connect(new InetSocketAddress(dns, DNS_SRV_PORT), DNS_SRV_TIMEOUT_MS);
            sock.setSoTimeout(DNS_SRV_TIMEOUT_MS);
            OutputStream out = sock.getOutputStream();
            InputStream  in  = sock.getInputStream();
            // TCP DNS: 2-byte big-endian length prefix
            out.write((query.length >> 8) & 0xFF);
            out.write(query.length & 0xFF);
            out.write(query);
            out.flush();
            int len  = ((in.read() & 0xFF) << 8) | (in.read() & 0xFF);
            byte[] resp = new byte[len];
            int read = 0;
            while (read < len) {
                int n = in.read(resp, read, len - read);
                if (n < 0) break;
                read += n;
            }
            return resp;
        }
    }

    public static byte[] buildDnsQuery(String name, int qtype) {
        byte[] qname  = encodeDnsName(name);
        byte[] packet = new byte[12 + qname.length + 4];
        int    id     = ThreadLocalRandom.current().nextInt(0x10000);
        packet[0] = (byte) (id >> 8);
        packet[1] = (byte) (id & 0xFF);
        packet[2] = 0x01; // RD = 1 (recursion desired)
        packet[4] = 0x00; packet[5] = 0x01; // QDCOUNT = 1
        System.arraycopy(qname, 0, packet, 12, qname.length);
        int off = 12 + qname.length;
        packet[off]   = (byte) (qtype >> 8);
        packet[off+1] = (byte) (qtype & 0xFF);
        packet[off+3] = 0x01; // QCLASS = IN
        return packet;
    }

    public static byte[] encodeDnsName(String name) {
        String[] labels = name.split("\\.");
        int size = 1; // trailing null byte
        for (String l : labels) size += 1 + l.length();
        byte[] buf = new byte[size];
        int pos = 0;
        for (String l : labels) {
            byte[] lb = l.getBytes(StandardCharsets.US_ASCII);
            buf[pos++] = (byte) lb.length;
            System.arraycopy(lb, 0, buf, pos, lb.length);
            pos += lb.length;
        }
        return buf; // buf[pos] == 0 (null terminator, already zero from new byte[])
    }

    /**
     * Parses an SRV DNS response and returns a list of [hostname, port] pairs.
     */
    public static List<String[]> parseSrvRecords(byte[] data) throws Exception {
        if (data.length < 12) throw new Exception("DNS response too short (" + data.length + " bytes)");
        int rcode = data[3] & 0x0F;
        if (rcode != 0) throw new Exception("DNS RCODE=" + rcode + " error for SRV query");

        int qdCount = dnsShort(data, 4);
        int anCount = dnsShort(data, 6);
        int offset  = 12;

        // Skip question section
        for (int i = 0; i < qdCount && offset < data.length; i++) {
            offset = dnsSkipName(data, offset) + 4; // +4 for QTYPE + QCLASS
        }

        List<String[]> results = new ArrayList<>();
        for (int i = 0; i < anCount && offset + 10 <= data.length; i++) {
            offset  = dnsSkipName(data, offset);     // NAME field
            int type     = dnsShort(data, offset);
            int rdLength = dnsShort(data, offset + 8); // TYPE(2)+CLASS(2)+TTL(4) = 8
            offset += 10;                              // past TYPE+CLASS+TTL+RDLENGTH

            if (offset + rdLength > data.length) break; // malformed

            if (type == 33 /* SRV */ && rdLength >= 7) {
                int    port     = dnsShort(data, offset + 4);
                int[]  nameOff  = {offset + 6};
                String target   = dnsDecodeName(data, nameOff);
                results.add(new String[]{target, String.valueOf(port)});
            }
            offset += rdLength;
        }
        return results;
    }

    public static int dnsShort(byte[] data, int off) {
        return ((data[off] & 0xFF) << 8) | (data[off + 1] & 0xFF);
    }

    /** Skips a DNS-encoded name and returns the offset of the first byte after it. */
    static int dnsSkipName(byte[] data, int offset) {
        while (offset < data.length) {
            int len = data[offset] & 0xFF;
            if (len == 0) return offset + 1;
            if ((len & 0xC0) == 0xC0) return offset + 2; // compression pointer
            offset += 1 + len;
        }
        return offset;
    }

    /**
     * Decodes a DNS-encoded name (supports compression pointers).
     * {@code offsetHolder[0]} is updated to point past the name in the original buffer.
     */
    static String dnsDecodeName(byte[] data, int[] offsetHolder) {
        StringBuilder sb       = new StringBuilder();
        int           off      = offsetHolder[0];
        boolean       jumped   = false;
        int           savedNext = -1;
        int           hops     = 0;

        while (off < data.length) {
            int len = data[off] & 0xFF;
            if (len == 0) {
                if (!jumped) offsetHolder[0] = off + 1;
                else         offsetHolder[0] = savedNext;
                break;
            }
            if ((len & 0xC0) == 0xC0) {
                if (!jumped) savedNext = off + 2;
                off    = ((len & 0x3F) << 8) | (data[off + 1] & 0xFF);
                jumped = true;
                if (++hops > 20) { offsetHolder[0] = savedNext < 0 ? off : savedNext; break; }
                continue;
            }
            if (sb.length() > 0) sb.append('.');
            sb.append(new String(data, off + 1, len, StandardCharsets.US_ASCII));
            off += 1 + len;
        }
        return sb.toString();
    }
}
