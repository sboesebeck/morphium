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
package de.caluga.morphium.quarkus.messaging;

import io.quarkus.runtime.annotations.ConfigGroup;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * Settings of the injected {@code MorphiumMessaging} (MOR-033), {@code quarkus.morphium.messaging.*}. Every property is optional: what is not set stays what Morphium's own
 * default is.
 */
@ConfigGroup
public interface MorphiumMessagingConfig {

    /** Name of the message queue (the collection that holds the messages). */
    Optional<String> queueName();

    /** How many messages one poll takes into processing (the window size). */
    OptionalInt windowSize();

    /** Pause between two polls in milliseconds. */
    OptionalInt pollPause();

    /** Process the messages of a poll in several threads. */
    Optional<Boolean> multithreaded();

    /** Use a change stream to be told about new messages instead of polling only. */
    Optional<Boolean> useChangeStream();

    /** The id under which this instance sends and receives; random if not set. */
    Optional<String> senderId();

    /** Answer a message automatically with an empty answer when its listener returns none. */
    Optional<Boolean> autoAnswer();
}
