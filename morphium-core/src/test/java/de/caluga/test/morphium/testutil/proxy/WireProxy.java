package de.caluga.test.morphium.testutil.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

/**
 * A TCP proxy that forwards MongoDB wire protocol traffic between a client and one backend
 * host:port, with runtime-switchable fault injection ({@link FaultMode}), an optional
 * {@link ResponseRewriter} for backend→client frames, and read-only {@link FrameObserver}s.
 * See docs/superpowers/specs/2026-08-05-failover-proxy-test-design.md for the full design.
 *
 * Client→backend frames are forwarded raw (length-prefixed, unparsed); only backend→client
 * frames are parsed (needed for rewrite/observation) - see the design spec's
 * "Pass-through fidelity" for why.
 */
public class WireProxy implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WireProxy.class);

    private final String backendHost;
    private final int backendPort;
    private final ServerSocket listener;
    private final AtomicReference<FaultMode> faultMode = new AtomicReference<>(FaultMode.passthrough);
    private final List<FrameObserver> observers = new CopyOnWriteArrayList<>();
    private final List<Socket> liveSockets = new CopyOnWriteArrayList<>();
    /** Every pump thread {@link #startForwarding} spawns, tracked so {@link #stop()} can join
     * them all before returning (design spec: teardown must guarantee every pump thread exits
     * before the test method returns) - closing the sockets only unblocks them asynchronously,
     * it doesn't wait for them to actually finish. */
    private final List<Thread> pumpThreads = new CopyOnWriteArrayList<>();
    private volatile ResponseRewriter rewriter;
    private volatile boolean running;
    private Thread acceptThread;

    public WireProxy(String backendHost, int backendPort) throws IOException {
        this.backendHost = backendHost;
        this.backendPort = backendPort;
        this.listener = new ServerSocket();
        this.listener.bind(new InetSocketAddress("localhost", 0));
    }

    public int getListenPort() {
        return listener.getLocalPort();
    }

    public void setFaultMode(FaultMode mode) {
        faultMode.set(mode);
    }

    public FaultMode getFaultMode() {
        return faultMode.get();
    }

    public void setRewriter(ResponseRewriter rewriter) {
        this.rewriter = rewriter;
    }

    public void addObserver(FrameObserver observer) {
        observers.add(observer);
    }

    public void start() {
        running = true;
        acceptThread = new Thread(this::acceptLoop, "wireproxy-accept-" + getListenPort());
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running) {
            Socket client;
            try {
                client = listener.accept();
            } catch (IOException e) {
                return; // listener closed - stop() was called
            }
            handleNewConnection(client);
        }
    }

    private void handleNewConnection(Socket client) {
        FaultMode mode = faultMode.get();
        if (mode == FaultMode.reset || mode == FaultMode.close) {
            // Refuse outright: matches "nothing reachable behind this proxy right now" -
            // see the design spec's "New connection attempts during a fault".
            sever(client, mode);
            return;
        }
        if (mode == FaultMode.freeze) {
            // Accept, then never touch this socket again until stop() - matches kill -STOP:
            // the OS completes the handshake from its backlog, nothing ever answers. This is
            // deliberately one-way/permanent for THIS connection: even if the fault mode later
            // switches back to passthrough, a connection accepted here is never picked up and
            // forwarded - it stays parked until stop() severs it. Freeze is only reversible in
            // the sense that new connections accepted after the mode change get passthrough
            // treatment; it is not reversible per-connection.
            liveSockets.add(client);
            return;
        }
        liveSockets.add(client);
        startForwarding(client);
    }

    private void startForwarding(Socket client) {
        if (!running) {
            // stop() raced us between accept() and here: this connection was never registered
            // in liveSockets in time to be severed by stop()'s own loop, and no backend
            // connection/pump thread exists yet to do it later - sever it ourselves, forced,
            // so the client always sees a hard reset rather than silently hanging or getting a
            // passthrough response after the proxy is supposed to be down.
            sever(client, FaultMode.reset);
            return;
        }
        Socket backend;
        try {
            backend = new Socket();
            backend.connect(new InetSocketAddress(backendHost, backendPort), 5000);
        } catch (IOException e) {
            log.warn("could not connect to backend {}:{}", backendHost, backendPort, e);
            closeQuietly(client);
            return;
        }
        liveSockets.add(backend);

        Thread toBackend = new Thread(() -> pumpClientToBackend(client, backend),
                "wireproxy-c2b-" + getListenPort());
        Thread toClient = new Thread(() -> pumpBackendToClient(backend, client),
                "wireproxy-b2c-" + getListenPort());
        toBackend.setDaemon(true);
        toClient.setDaemon(true);
        pumpThreads.add(toBackend);
        pumpThreads.add(toClient);
        toBackend.start();
        toClient.start();
    }

    private void pumpClientToBackend(Socket client, Socket backend) {
        try {
            InputStream in = client.getInputStream();
            OutputStream out = backend.getOutputStream();
            byte[] header = new byte[16];
            while (running) {
                FaultMode mode = faultMode.get();
                if (mode == FaultMode.freeze) {
                    parkWhileFrozen();
                    continue;
                }
                if (mode == FaultMode.reset || mode == FaultMode.close) {
                    return; // finally below severs both sockets per current mode
                }
                if (!readFully(in, header, 16)) return;
                int size = WireProtocolMessage.readInt(header, 0);
                if (size < 16) return; // desynced/corrupt - stop
                byte[] body = new byte[size - 16];
                if (!readFully(in, body, body.length)) return;
                if (faultMode.get() != FaultMode.passthrough) continue; // fault kicked in mid-read: drop this frame
                out.write(header);
                out.write(body);
                out.flush();
            }
        } catch (IOException ignored) {
        } finally {
            // If stop() has already flipped running to false, force a reset regardless of
            // whatever fault mode happens to be configured (typically passthrough by default) -
            // otherwise a pump thread that wakes up during/after stop() would sever with a plain
            // close() and the client would see a clean EOF instead of the hard reset stop()
            // promises.
            sever(client, running ? faultMode.get() : FaultMode.reset);
            closeQuietly(backend);
        }
    }

    private void pumpBackendToClient(Socket backend, Socket client) {
        try {
            InputStream in = backend.getInputStream();
            OutputStream out = client.getOutputStream();
            while (running) {
                FaultMode mode = faultMode.get();
                if (mode == FaultMode.freeze) {
                    parkWhileFrozen();
                    continue;
                }
                if (mode == FaultMode.reset || mode == FaultMode.close) {
                    return;
                }
                WireProtocolMessage msg = WireProtocolMessage.parseFromStream(in);
                if (msg == null) return; // backend closed
                if (!observers.isEmpty()) {
                    // client.getRemoteSocketAddress() can NPE if the client socket already
                    // closed between the parse above and here (e.g. a concurrent stop()/fault
                    // severing it) - an observer registration must never crash the pump thread
                    // over what is purely diagnostic context.
                    String peer;
                    try {
                        peer = String.valueOf(client.getRemoteSocketAddress());
                    } catch (Exception e) {
                        peer = "<client socket closed>";
                    }
                    ConnectionCtx ctx = new ConnectionCtx(peer, getListenPort());
                    for (FrameObserver o : observers) {
                        o.onFrame(FrameObserver.Direction.BACKEND_TO_CLIENT, msg, ctx);
                    }
                }
                if (faultMode.get() != FaultMode.passthrough) continue; // fault kicked in mid-read: drop this frame
                ResponseRewriter rw = rewriter;
                if (rw != null) {
                    msg = rw.rewrite(msg);
                }
                out.write(msg.bytes());
                out.flush();
            }
        } catch (IOException ignored) {
        } finally {
            // See the matching comment in pumpClientToBackend's finally block.
            sever(client, running ? faultMode.get() : FaultMode.reset);
            closeQuietly(backend);
        }
    }

    /** Spins while frozen, touching neither socket, so the client sees no data and no close -
     * exactly the freeze contract. Returns once the fault mode changes or the proxy stops. */
    private void parkWhileFrozen() {
        while (running && faultMode.get() == FaultMode.freeze) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean readFully(InputStream in, byte[] buf, int len) throws IOException {
        int read = 0;
        while (read < len) {
            int r = in.read(buf, read, len - read);
            if (r == -1) return false;
            read += r;
        }
        return true;
    }

    /** Closes with RST if the current mode is {@code reset}, otherwise a normal FIN close. */
    private void sever(Socket s, FaultMode mode) {
        try {
            if (mode == FaultMode.reset) {
                s.setSoLinger(true, 0);
            }
        } catch (IOException ignored) {
        }
        closeQuietly(s);
    }

    private void closeQuietly(Socket s) {
        try { s.close(); } catch (IOException ignored) { }
        liveSockets.remove(s);
    }

    /** Stops accepting new connections and severs every live socket with a hard reset so
     * blocked pump threads unblock (a parseFromStream()/read() only returns once its socket is
     * closed) and any peer still reading sees a definite error rather than a clean EOF, which
     * on its own is indistinguishable from an orderly shutdown. */
    public void stop() {
        running = false;
        try {
            listener.close();
        } catch (IOException ignored) {
        }
        for (Socket s : liveSockets) {
            sever(s, FaultMode.reset);
        }
        liveSockets.clear();
        if (acceptThread != null) {
            try {
                acceptThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // The sever() calls above only unblock the pump threads asynchronously (their read()/
        // parseFromStream() calls return once the socket is actually closed) - join them here so
        // stop() guarantees every pump thread has exited before it returns (design spec).
        for (Thread t : pumpThreads) {
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        pumpThreads.clear();
    }

    @Override
    public void close() {
        stop();
    }
}
