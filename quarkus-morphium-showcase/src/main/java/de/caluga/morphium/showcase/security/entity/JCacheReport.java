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
package de.caluga.morphium.showcase.security.entity;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.driver.MorphiumId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

/**
 * A deliberately simple entity used to demonstrate Morphium's <strong>JCache (JSR-107)</strong>
 * cache integration.
 *
 * <p>{@code @Cache} activation is identical whether the underlying implementation is the
 * default {@code MorphiumCacheImpl} or the JSR-107-backed {@code MorphiumCacheJCacheImpl} --
 * switching implementations is a {@code cacheSettings().setCache(...)} choice made once per
 * {@code Morphium} instance, not an annotation attribute. See
 * {@link de.caluga.morphium.showcase.security.JCacheService} for where that switch happens.</p>
 */
@Entity(collectionName = "jcache_reports")
@Cache(timeout = 60000, maxEntries = 100, readCache = true, clearOnWrite = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldNameConstants
public class JCacheReport {

    @Id
    private MorphiumId id;

    private String title;
    private double revenue;
}
