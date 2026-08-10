package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Registry of the operations this server is currently executing - the data source for
 * currentOp / the $currentOp aggregation stage, and the target of killOp.
 *
 * Every OpMsg dispatch registers itself for the duration of its synchronous processing.
 * Write-concern waits continue on the command executor after dispatch returns and
 * asynchronous continuations (deferred getMore responses) are not tracked - currentOp
 * shows what a thread is actively working on right now.
 *
 * killOp is cooperative like mongod's: it marks the op (visible as killPending) and
 * best-effort interrupts the executing thread - but never a Netty event loop, which
 * must not be interrupted. Ops running on the event loop only get the flag.
 */
public class OpRegistry {

    /** Commands whose document must not leak through currentOp (credentials/payloads). */
    private static final Set<String> REDACTED_COMMANDS = Set.of("saslstart", "saslcontinue", "createuser", "updateuser");

    private final AtomicLong opIdSeq = new AtomicLong(1);
    private final Map<Long, OpEntry> active = new ConcurrentHashMap<>();

    public static final class OpEntry {
        private final long opid;
        private final String commandName;
        private final String ns;
        private final Map<String, Object> command;
        private final long startedAtMs;
        private final String client;
        private final Thread thread;
        private volatile boolean killPending;

        private OpEntry(long opid, String commandName, String ns, Map<String, Object> command,
                        long startedAtMs, String client, Thread thread) {
            this.opid = opid;
            this.commandName = commandName;
            this.ns = ns;
            this.command = command;
            this.startedAtMs = startedAtMs;
            this.client = client;
            this.thread = thread;
        }

        public long getOpid() {
            return opid;
        }

        public boolean isKillPending() {
            return killPending;
        }
    }

    public OpEntry register(String commandName, Map<String, Object> commandDoc, String client) {
        long id = opIdSeq.getAndIncrement();
        String db = commandDoc.get("$db") instanceof String s ? s : "admin";
        Object collValue = commandDoc.get(commandName);
        String ns = collValue instanceof String coll ? db + "." + coll : db;
        Map<String, Object> reported = REDACTED_COMMANDS.contains(commandName.toLowerCase())
            ? Doc.of(commandName, "***redacted***")
            : commandDoc;
        OpEntry e = new OpEntry(id, commandName, ns, reported, System.currentTimeMillis(),
                                client, Thread.currentThread());
        active.put(id, e);
        return e;
    }

    public void deregister(OpEntry entry) {
        if (entry != null) {
            active.remove(entry.opid);
        }
    }

    /**
     * Mark the op as kill-pending and interrupt its thread if that is safe (never a Netty
     * event loop). Returns false when no such op is active.
     */
    public boolean killOp(long opid) {
        OpEntry e = active.get(opid);

        if (e == null) {
            return false;
        }

        e.killPending = true;
        Thread t = e.thread;

        if (t != null && t.isAlive() && !t.getName().toLowerCase().contains("eventloop")) {
            t.interrupt();
        }

        return true;
    }

    /** mongod-shaped $currentOp documents for every active op, ordered by opid. */
    public List<Map<String, Object>> snapshot() {
        long now = System.currentTimeMillis();
        List<OpEntry> entries = new ArrayList<>(active.values());
        entries.sort(Comparator.comparingLong(e -> e.opid));
        List<Map<String, Object>> out = new ArrayList<>(entries.size());

        for (OpEntry e : entries) {
            long runMs = Math.max(0, now - e.startedAtMs);
            Doc d = Doc.of();
            d.put("type", "op");
            d.put("desc", "conn");
            d.put("active", true);
            d.put("opid", e.opid);
            d.put("op", opTypeFor(e.commandName));
            d.put("ns", e.ns);
            d.put("command", e.command);
            d.put("secs_running", runMs / 1000);
            d.put("microsecs_running", runMs * 1000);
            d.put("client", e.client);
            d.put("appName", "");
            d.put("killPending", e.killPending);
            d.put("currentOpTime", new Date(now));
            out.add(d);
        }

        return out;
    }

    private static String opTypeFor(String commandName) {
        return switch (commandName.toLowerCase()) {
            case "insert" -> "insert";
            case "update" -> "update";
            case "delete" -> "remove";
            case "find" -> "query";
            case "getmore" -> "getmore";
            default -> "command";
        };
    }
}
