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

import de.caluga.morphium.changestream.ChangeStreamEvent;

import java.time.Instant;
import java.util.Map;

/**
 * Flattened, JSON/Qute-friendly view of a {@link ChangeStreamEvent}, used to push change
 * notifications to the browser via Server-Sent Events (SSE) and to render the live feed
 * in the Qute demo template.
 *
 * <p>{@link ChangeStreamEvent} itself carries raw MongoDB change stream fields (BSON-ish
 * maps, resume tokens, etc.) that are awkward to serialize directly. This record extracts
 * only the pieces relevant for the demo UI.</p>
 *
 * @param operationType MongoDB's operation type: {@code insert}, {@code update}, {@code replace},
 *                       {@code delete}, {@code drop}, ...
 * @param documentId     string form of the changed document's {@code _id}
 * @param fullDocument   the full document after the change (present for insert/update/replace
 *                        when {@code fullDocument=true} was requested on the watch)
 * @param updatedFields  for {@code update} events: the fields that changed and their new values
 * @param removedFields  for {@code update} events: field names that were removed
 * @param receivedAt     local server timestamp when this event was processed (for display only)
 */
public record ChangeEventView(
        String operationType,
        String documentId,
        Map<String, Object> fullDocument,
        Map<String, Object> updatedFields,
        Object removedFields,
        String receivedAt) {

    /**
     * Builds a {@link ChangeEventView} from a raw Morphium {@link ChangeStreamEvent}.
     */
    public static ChangeEventView from(ChangeStreamEvent evt) {
        String docId = null;
        if (evt.getDocumentKey() instanceof Map<?, ?> key && key.get("_id") != null) {
            docId = String.valueOf(key.get("_id"));
        } else if (evt.getDocumentKey() != null) {
            docId = String.valueOf(evt.getDocumentKey());
        }

        return new ChangeEventView(
                evt.getOperationType(),
                docId,
                evt.getFullDocument(),
                evt.getUpdatedFields(),
                evt.getRemovedFields(),
                Instant.now().toString());
    }
}
