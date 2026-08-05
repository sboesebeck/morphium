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
package de.caluga.morphium.quarkus.transaction;

import de.caluga.morphium.driver.MorphiumDriverException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the transient-error detection logic in
 * {@link MorphiumTransactionalInterceptor}.
 *
 * <p>These tests exercise {@code isTransientTransactionError} directly —
 * no Quarkus container or MongoDB connection is required.
 */
@DisplayName("MorphiumTransactionalInterceptor – transient error detection")
class MorphiumTransactionalInterceptorRetryTest {

    // -------------------------------------------------------------------------
    // Helper: build a MorphiumDriverException with a numeric mongo error code
    // -------------------------------------------------------------------------

    private static MorphiumDriverException exceptionWithCode(int code) {
        MorphiumDriverException ex = new MorphiumDriverException("mongo error " + code);
        ex.setMongoCode(code);
        return ex;
    }

    // -------------------------------------------------------------------------
    // Transient codes — should trigger retry
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("code 112 (WriteConflict) is transient")
    void writeConflict_isTransient() {
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(exceptionWithCode(112)))
                .isTrue();
    }

    @Test
    @DisplayName("code 251 (NoSuchTransaction) is transient")
    void noSuchTransaction_isTransient() {
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(exceptionWithCode(251)))
                .isTrue();
    }

    // -------------------------------------------------------------------------
    // Non-transient codes — must NOT retry
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("code 11000 (DuplicateKey) is NOT transient")
    void duplicateKey_isNotTransient() {
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(exceptionWithCode(11000)))
                .isFalse();
    }

    @Test
    @DisplayName("code 0 is NOT transient")
    void zeroCode_isNotTransient() {
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(exceptionWithCode(0)))
                .isFalse();
    }

    @Test
    @DisplayName("MorphiumDriverException with no mongoCode set is NOT transient")
    void noCodeSet_isNotTransient() {
        MorphiumDriverException ex = new MorphiumDriverException("no code");
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(ex))
                .isFalse();
    }

    // -------------------------------------------------------------------------
    // Non-MorphiumDriverException — must NOT retry
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("plain RuntimeException is NOT transient")
    void plainRuntimeException_isNotTransient() {
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(
                new RuntimeException("forced rollback")))
                .isFalse();
    }

    @Test
    @DisplayName("IllegalStateException is NOT transient")
    void illegalStateException_isNotTransient() {
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(
                new IllegalStateException("bad state")))
                .isFalse();
    }

    // -------------------------------------------------------------------------
    // Wrapped / cause-chain detection
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("WriteConflict (112) wrapped in RuntimeException IS detected as transient")
    void writeConflict_wrappedInRuntimeException_isTransient() {
        MorphiumDriverException cause = exceptionWithCode(112);
        RuntimeException wrapper = new RuntimeException("wrapper", cause);
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(wrapper))
                .isTrue();
    }

    @Test
    @DisplayName("NoSuchTransaction (251) wrapped two levels deep IS detected as transient")
    void noSuchTransaction_deeplyWrapped_isTransient() {
        MorphiumDriverException root = exceptionWithCode(251);
        RuntimeException mid = new RuntimeException("mid", root);
        RuntimeException outer = new RuntimeException("outer", mid);
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(outer))
                .isTrue();
    }

    @Test
    @DisplayName("DuplicateKey (11000) wrapped in RuntimeException is NOT transient")
    void duplicateKey_wrappedInRuntimeException_isNotTransient() {
        MorphiumDriverException cause = exceptionWithCode(11000);
        RuntimeException wrapper = new RuntimeException("wrapper", cause);
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(wrapper))
                .isFalse();
    }

    // -------------------------------------------------------------------------
    // mongoCode as Long (Number subtype other than Integer)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("code 112 stored as Long is still detected as transient")
    void writeConflict_asLong_isTransient() {
        MorphiumDriverException ex = new MorphiumDriverException("write conflict via long");
        ex.setMongoCode(112L);
        assertThat(MorphiumTransactionalInterceptor.isTransientTransactionError(ex))
                .isTrue();
    }
}
