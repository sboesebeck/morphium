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
package de.caluga.morphium.quarkus.it;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.quarkus.transaction.MorphiumTransactional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Test service exercising {@link MorphiumTransactional} for integration tests.
 */
@ApplicationScoped
public class TransactionalService {

    @Inject
    Morphium morphium;

    @MorphiumTransactional
    public void storeSuccessfully(ItemEntity item) {
        morphium.store(item);
    }

    @MorphiumTransactional
    public void storeAndFail(ItemEntity item) {
        morphium.store(item);
        throw new RuntimeException("forced rollback");
    }
}
