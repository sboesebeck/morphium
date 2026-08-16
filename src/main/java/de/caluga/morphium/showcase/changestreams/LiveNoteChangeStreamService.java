/*
 * Copyright 2025 The Quarkiverse Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.caluga.morphium.showcase.changestreams;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.changestream.ChangeStreamEvent;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.showcase.changestreams.entity.LiveNote;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Central Change Stream demo service: opens (and cleanly shuts down) a Morphium
 * {@link ChangeStreamMonitor} for the {@link LiveNote} collection and fans out every
 * incoming {@link ChangeStreamEvent} to whoever is currently subscribed -- most notably the
 * SSE endpoint in {@code ChangeStreamsResource}, which streams events into the browser.
 *
 * <h3>How Morphium Change Streams Work</h3>
 * <p>MongoDB Change Streams are built on top of the replication oplog. When you {@code watch()}
 * a collection (or an entire database), the server keeps a cursor open and pushes a new event
 * document for every insert/update/replace/delete/drop that happens -- there is no polling
 * involved on either side. Morphium wraps the raw driver command in
 * {@link ChangeStreamMonitor}, which:</p>
 * <ol>
 *   <li>Opens a dedicated connection and issues MongoDB's {@code aggregate} command with a
 *       {@code $changeStream} stage (via {@code WatchCommand}), scoped to a single collection
 *       when constructed with {@code new ChangeStreamMonitor(morphium, CollectionClass.class)}.</li>
 *   <li>Runs a background thread that blocks on the resulting tailable/change cursor and calls
 *       every registered {@link de.caluga.morphium.changestream.ChangeStreamListener#incomingData(ChangeStreamEvent)}
 *       for each incoming event.</li>
 *   <li>Automatically tracks a <em>resume token</em> so that if the watch has to reconnect
 *       (network blip, cursor timeout, ...), it resumes exactly where it left off instead of
 *       missing or duplicating events.</li>
 * </ol>
 *
 * <p>Listener registration is push-based: {@code incomingData()} returns {@code true} to keep
 * receiving events, or {@code false} to unsubscribe. This service keeps a
 * {@link CopyOnWriteArrayList} of SSE "subscriber" callbacks and always returns {@code true} so
 * the single underlying MongoDB watch stays alive for the whole application lifetime,
 * independent of how many browser tabs are currently connected.</p>
 *
 * <h3>Lifecycle</h3>
 * <p>The monitor is started once at application startup ({@code @Observes StartupEvent}) and
 * terminated cleanly on shutdown ({@code @Observes ShutdownEvent}) -- mirroring how
 * {@code ChangeStreamMonitor} is used throughout Morphium's own test suite and in
 * {@code SingleCollectionMessaging}.</p>
 *
 * @see ChangeStreamMonitor
 * @see ChangeStreamEvent
 * @see de.caluga.morphium.showcase.changestreams.ChangeStreamsResource
 */
@ApplicationScoped
public class LiveNoteChangeStreamService {

    /** Keep the last N events in memory so a freshly opened browser tab has something to show. */
    private static final int HISTORY_SIZE = 25;

    @Inject
    Morphium morphium;

    private ChangeStreamMonitor monitor;

    private final List<Consumer<ChangeEventView>> subscribers = new CopyOnWriteArrayList<>();
    private final Deque<ChangeEventView> recentEvents = new ArrayDeque<>();

    /**
     * Starts the change stream watch for the {@link LiveNote} collection when the application
     * boots.
     *
     * <p>{@code new ChangeStreamMonitor(morphium, LiveNote.class)} scopes the watch to exactly
     * the {@code live_notes} collection (Morphium resolves the collection name from the entity's
     * {@code @Entity} annotation). Passing no {@code Class} argument at all would watch the whole
     * database instead.</p>
     *
     * <p>{@code monitor.start()} blocks briefly (up to ~2s) until MongoDB has confirmed the
     * change stream cursor is registered, so that a write performed immediately after startup is
     * guaranteed not to race past the watch.</p>
     */
    void onStart(@Observes StartupEvent ev) {
        monitor = new ChangeStreamMonitor(morphium, LiveNote.class);
        monitor.addListener(this::handleEvent);
        monitor.start();
    }

    /**
     * Cleanly stops the change stream watch, closing its dedicated MongoDB connection.
     *
     * <p>{@code ChangeStreamMonitor.terminate()} interrupts the blocking watch loop, releases the
     * underlying connection back to the pool (or closes it if its state is unknown), and clears
     * all registered listeners. Without this, the background thread would keep the driver alive
     * past application shutdown.</p>
     */
    void onStop(@Observes ShutdownEvent ev) {
        if (monitor != null) {
            monitor.terminate();
        }
    }

    /**
     * Invoked by {@link ChangeStreamMonitor} for every insert/update/replace/delete event on the
     * watched collection. Converts the raw event into a {@link ChangeEventView}, stores it in a
     * bounded in-memory history (for late SSE subscribers), and pushes it to every currently
     * connected subscriber.
     *
     * @return always {@code true} -- this listener never wants to be automatically removed;
     *         the watch is meant to run for the entire application lifetime
     */
    private boolean handleEvent(ChangeStreamEvent evt) {
        // "drop"/"invalidate" events (e.g. from morphium.dropCollection()) have no document
        // payload worth showing in the demo feed, but we still surface them so users understand
        // why the feed might reset.
        ChangeEventView view = ChangeEventView.from(evt);

        synchronized (recentEvents) {
            recentEvents.addLast(view);
            while (recentEvents.size() > HISTORY_SIZE) {
                recentEvents.removeFirst();
            }
        }

        for (Consumer<ChangeEventView> subscriber : subscribers) {
            try {
                subscriber.accept(view);
            } catch (Exception e) {
                // A broken SSE connection should never take down the shared change stream watch.
                subscribers.remove(subscriber);
            }
        }
        return true;
    }

    /**
     * Registers a new subscriber (typically one SSE connection) that will receive every future
     * change event as it arrives. Returns a {@link Runnable} that unregisters it again.
     */
    public Runnable subscribe(Consumer<ChangeEventView> subscriber) {
        subscribers.add(subscriber);
        return () -> subscribers.remove(subscriber);
    }

    /** Returns the most recent events received so far, oldest first. */
    public List<ChangeEventView> recentHistory() {
        synchronized (recentEvents) {
            return List.copyOf(recentEvents);
        }
    }

    /** Number of browser tabs / SSE connections currently subscribed to the live feed. */
    public int subscriberCount() {
        return subscribers.size();
    }

    /** Whether the underlying MongoDB change stream watch is currently active. */
    public boolean isWatching() {
        return monitor != null && monitor.isRunning();
    }

    /** Clears the in-memory event history (does not affect MongoDB data). */
    public void clearHistory() {
        synchronized (recentEvents) {
            recentEvents.clear();
        }
    }
}
