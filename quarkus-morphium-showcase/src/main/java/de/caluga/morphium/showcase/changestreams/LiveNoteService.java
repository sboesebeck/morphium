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
import de.caluga.morphium.showcase.changestreams.entity.LiveNote;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;

/**
 * Plain CRUD service for {@link LiveNote}, deliberately kept separate from the change-stream
 * plumbing in {@link LiveNoteChangeStreamService}.
 *
 * <p>This class performs ordinary Morphium writes and queries -- {@code store()}, {@code set()},
 * {@code delete()}. It has <strong>no awareness</strong> of change streams whatsoever. That
 * separation is the whole point of the demo: any write made here (from this app, from a
 * different microservice, from the {@code mongosh} shell, ...) is picked up automatically by
 * the change stream watcher and pushed to connected browsers in realtime.</p>
 *
 * @see LiveNoteChangeStreamService the change-stream listener that reacts to writes made here
 */
@ApplicationScoped
public class LiveNoteService {

    @Inject
    Morphium morphium;

    /** Returns all notes, most recently updated first. */
    public List<LiveNote> findAll() {
        return morphium.createQueryFor(LiveNote.class)
                .sort(Map.of(LiveNote.Fields.updatedAt, -1))
                .asList();
    }

    /** Returns the total number of notes currently stored. */
    public long count() {
        return morphium.createQueryFor(LiveNote.class).countAll();
    }

    /**
     * Creates and stores a new note.
     *
     * <p>{@code morphium.store()} performs an INSERT because {@code id} is {@code null}.
     * This triggers an {@code insert} change stream event with {@code operationType=insert}
     * and (since {@code fullDocument} is always present on inserts) the complete new document.</p>
     */
    public LiveNote create(String title, String content, String author) {
        LiveNote note = LiveNote.builder()
                .title(title)
                .content(content)
                .author(author)
                .build();
        morphium.store(note);
        return note;
    }

    /**
     * Updates a note's content via a targeted field-level update.
     *
     * <p>{@code query.set()} triggers an {@code update} change stream event. Unlike a full
     * document replace, the event's {@code updateDescription.updatedFields} will contain only
     * the {@code content} and {@code updated_at} fields -- letting change-stream consumers apply
     * minimal, efficient patches instead of re-fetching the whole document.</p>
     */
    public void updateContent(String id, String newContent) {
        morphium.createQueryFor(LiveNote.class)
                .f(LiveNote.Fields.id).eq(new de.caluga.morphium.driver.MorphiumId(id))
                .set(LiveNote.Fields.content, newContent, false, false, null);
    }

    /**
     * Deletes a note by id.
     *
     * <p>Triggers a {@code delete} change stream event. Delete events never carry a
     * {@code fullDocument} (the document is gone) -- only the {@code documentKey} ({@code _id})
     * of what was removed.</p>
     */
    public void delete(String id) {
        morphium.createQueryFor(LiveNote.class)
                .f(LiveNote.Fields.id).eq(new de.caluga.morphium.driver.MorphiumId(id))
                .delete();
    }

    /** Removes all notes -- used by the demo's "reset" action. */
    public void deleteAll() {
        morphium.createQueryFor(LiveNote.class).delete();
    }
}
