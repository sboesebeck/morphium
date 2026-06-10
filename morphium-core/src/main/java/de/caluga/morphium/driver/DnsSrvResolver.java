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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for resolving MongoDB SRV records via pure-Java DNS (no JNDI).
 * Works in any JVM environment including Quarkus native, Android, and Windows.
 */
public final class DnsSrvResolver {

    private static final Logger log = LoggerFactory.getLogger(DnsSrvResolver.class);
    private static final int DNS_SRV_PORT       = 53;
    private static final int DNS_SRV_TIMEOUT_MS = 5_000;
    /** Public DNS resolvers used only as a last resort when no system name-servers are configured. */
    private static final String[] PUBLIC_DNS_FALLBACK = {"8.8.8.8", "1.1.1.1"};

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
        log.info("Resolving SRV '{}' using {} DNS server(s)", srvName, servers.size());
        Exception lastEx = null;
        for (InetAddress dns : servers) {
            log.debug("Querying DNS server {} for SRV '{}'", dns.getHostAddress(), srvName);
            try {
                List<String[]> records = srvQuery(dns, srvName);
                if (!records.isEmpty()) {
                    List<String> result = new ArrayList<>(records.size());
                    for (String[] hp : records) {
                        result.add(hp[0] + ":" + hp[1]);
                    }
                    log.info("DNS server {} returned {} SRV record(s) for '{}'", dns.getHostAddress(), result.size(), srvName);
                    for (String hostPort : result) {
                        log.info("  SRV record: {}", hostPort);
                    }
                    return result;
                }
                log.warn("DNS server {} returned no SRV records for '{}'", dns.getHostAddress(), srvName);
            } catch (Exception ex) {
                lastEx = ex;
                log.warn("DNS server {} failed for '{}': {}", dns.getHostAddress(), srvName, ex.getMessage());
            }
        }
        if (lastEx != null) throw lastEx;
        return Collections.emptyList();
    }

    /**
     * Resolves TXT records for the given DNS name (used for MongoDB seedlist options).
     * Per the DNS Seedlist Discovery spec the TXT record sits at the <em>bare</em> hostname
     * (e.g. {@code cluster.mongodb.net}), not under the {@code _mongodb._tcp.} prefix.
     * Returns the raw TXT strings; failures are swallowed and yield an empty list, since TXT
     * options are optional defaults and must never block a connection.
     *
     * @param name the bare hostname, e.g. {@code cluster.mongodb.net}
     * @return list of TXT record strings (never {@code null}, possibly empty)
     */
    public static List<String> resolveTxt(String name) {
        List<InetAddress> servers;
        try {
            servers = systemDnsServers();
        } catch (Exception ex) {
            log.debug("No DNS servers available for TXT lookup of '{}': {}", name, ex.getMessage());
            return Collections.emptyList();
        }
        for (InetAddress dns : servers) {
            try {
                byte[] query    = buildDnsQuery(name, 16 /* TXT */);
                byte[] response = dnsOverUdp(dns, query);
                if ((response[2] & 0x02) != 0) { // truncated → retry over TCP
                    response = dnsOverTcp(dns, query);
                }
                List<String> records = parseTxtRecords(response);
                if (!records.isEmpty()) {
                    log.info("DNS server {} returned {} TXT record(s) for '{}'", dns.getHostAddress(), records.size(), name);
                    return records;
                }
            } catch (Exception ex) {
                log.debug("DNS server {} TXT lookup failed for '{}': {}", dns.getHostAddress(), name, ex.getMessage());
            }
        }
        return Collections.emptyList();
    }

    /**
     * Collects name-server addresses from JVM properties and {@code /etc/resolv.conf}, falling back to
     * public DNS servers only when no system name-servers can be found.
     */
    public static List<InetAddress> systemDnsServers() {
        return systemDnsServers(new File("/etc/resolv.conf"));
    }

    /** Seam taking the resolv.conf location so the fallback logic is testable (consistent with the other public test seams in this class). */
    public static List<InetAddress> systemDnsServers(File resolvConf) {
        List<InetAddress> servers = collectConfiguredDnsServers(resolvConf);

        if (servers.isEmpty()) {
            // No system name-servers (e.g. minimal container without /etc/resolv.conf): use public DNS as a
            // last resort. Doing this unconditionally breaks split-DNS/private-Atlas setups and causes a
            // per-server timeout when outbound UDP/53 is firewalled (issue #170).
            log.debug("No system DNS name-servers found; falling back to public DNS {}", Arrays.toString(PUBLIC_DNS_FALLBACK));
            for (String addr : PUBLIC_DNS_FALLBACK) {
                try { servers.add(InetAddress.getByName(addr)); } catch (Exception ignored) {}
            }
        }

        if (log.isDebugEnabled()) {
            List<String> addrs = new ArrayList<>(servers.size());
            for (InetAddress s : servers) addrs.add(s.getHostAddress());
            log.debug("System DNS servers: {}", addrs);
        }
        return servers;
    }

    /** Reads configured name-servers from the {@code sun.net.spi.nameservice.nameservers} property and resolv.conf. */
    private static List<InetAddress> collectConfiguredDnsServers(File resolvConf) {
        List<InetAddress> servers = new ArrayList<>();

        String prop = System.getProperty("sun.net.spi.nameservice.nameservers");
        if (prop != null) {
            for (String s : prop.split(",")) {
                try { servers.add(InetAddress.getByName(s.trim())); } catch (Exception ignored) {}
            }
        }

        // resolv.conf only exists on Unix-like systems; the exists() check keeps this a no-op on Windows.
        if (resolvConf != null && resolvConf.exists()) {
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
        return servers;
    }

    /** Sends a DNS SRV query over UDP; retries over TCP if the response is truncated. */
    private static List<String[]> srvQuery(InetAddress dns, String srvName) throws Exception {
        byte[] query    = buildDnsQuery(srvName, 33 /* SRV */);
        byte[] response = dnsOverUdp(dns, query);
        // TC bit (byte 2, bit 1) set → response was truncated, retry over TCP
        if ((response[2] & 0x02) != 0) {
            log.info("DNS UDP response truncated ({} bytes), retrying over TCP for '{}'", response.length, srvName);
            response = dnsOverTcp(dns, query);
        }
        if (log.isTraceEnabled()) {
            log.trace("DNS raw response ({} bytes): {}", response.length, bytesToHex(response));
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
        log.debug("DNS response: {} bytes, RCODE={}, questions={}, answers={}", data.length, rcode, qdCount, anCount);
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

            if (offset + rdLength > data.length) {
                log.warn("DNS response: malformed record at offset {}, rdLength={} exceeds data length {}", offset, rdLength, data.length);
                break;
            }

            if (type == 33 /* SRV */ && rdLength >= 7) {
                int    priority = dnsShort(data, offset);
                int    weight   = dnsShort(data, offset + 2);
                int    port     = dnsShort(data, offset + 4);
                int[]  nameOff  = {offset + 6};
                String target   = dnsDecodeName(data, nameOff);
                log.debug("  SRV answer[{}]: priority={}, weight={}, port={}, target='{}'", i, priority, weight, port, target);
                results.add(new String[]{target, String.valueOf(port)});
            } else {
                log.debug("  DNS answer[{}]: type={}, rdLength={} (skipped, not SRV)", i, type, rdLength);
            }
            offset += rdLength;
        }
        return results;
    }

    /**
     * Parses a TXT DNS response and returns the decoded record strings.
     * Each TXT record's RDATA is one or more length-prefixed character-strings, which are
     * concatenated to form the full record value (per RFC 1035 §3.3.14).
     */
    public static List<String> parseTxtRecords(byte[] data) throws Exception {
        if (data.length < 12) throw new Exception("DNS response too short (" + data.length + " bytes)");
        int rcode = data[3] & 0x0F;
        if (rcode != 0) throw new Exception("DNS RCODE=" + rcode + " error for TXT query");

        int qdCount = dnsShort(data, 4);
        int anCount = dnsShort(data, 6);
        int offset  = 12;

        for (int i = 0; i < qdCount && offset < data.length; i++) {
            offset = dnsSkipName(data, offset) + 4; // +4 for QTYPE + QCLASS
        }

        List<String> results = new ArrayList<>();
        for (int i = 0; i < anCount && offset + 10 <= data.length; i++) {
            offset  = dnsSkipName(data, offset);
            int type     = dnsShort(data, offset);
            int rdLength = dnsShort(data, offset + 8);
            offset += 10;

            if (offset + rdLength > data.length) {
                log.warn("DNS response: malformed TXT record at offset {}, rdLength={} exceeds data length {}", offset, rdLength, data.length);
                break;
            }

            if (type == 16 /* TXT */) {
                StringBuilder sb  = new StringBuilder();
                int rd            = offset;
                int rdEnd         = offset + rdLength;
                while (rd < rdEnd) {
                    int strLen = data[rd] & 0xFF;
                    rd++;
                    if (rd + strLen > rdEnd) break; // malformed character-string
                    sb.append(new String(data, rd, strLen, StandardCharsets.UTF_8));
                    rd += strLen;
                }
                results.add(sb.toString());
            }
            offset += rdLength;
        }
        return results;
    }

    /**
     * Parses MongoDB seedlist TXT records (e.g. {@code "authSource=admin&replicaSet=myRS"}) into a
     * map of options. Keys are lower-cased so look-ups are case-insensitive (as connection-string
     * options are), while values keep their original case (e.g. replica-set names are case-sensitive).
     * Malformed fragments without a {@code key=value} shape or with an empty key are skipped.
     */
    public static Map<String, String> parseTxtOptions(List<String> txtRecords) {
        Map<String, String> options = new LinkedHashMap<>();
        if (txtRecords == null) return options;
        for (String record : txtRecords) {
            if (record == null || record.isBlank()) continue;
            for (String pair : record.split("&")) {
                int eq = pair.indexOf('=');
                if (eq <= 0) continue; // no '=' or empty key
                String key   = pair.substring(0, eq).trim().toLowerCase();
                String value = pair.substring(eq + 1).trim();
                if (key.isEmpty()) continue;
                options.put(key, value);
            }
        }
        return options;
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

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) sb.append(' ');
            sb.append(String.format("%02x", bytes[i] & 0xFF));
        }
        return sb.toString();
    }
}
