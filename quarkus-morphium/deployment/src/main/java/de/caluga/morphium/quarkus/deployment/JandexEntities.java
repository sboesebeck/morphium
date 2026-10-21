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
package de.caluga.morphium.quarkus.deployment;

import de.caluga.morphium.data.JdqlDelete;
import de.caluga.morphium.data.JdqlEntityName;
import de.caluga.morphium.data.JdqlUpdate;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Which entity a method of a repository works on (MOR-077), at build time: the rules of {@code de.caluga.morphium.data.RepositoryEntities}, applied to the Jandex types. A repository that
 * extends nothing has the entity of its lifecycle methods ({@code @Insert}, {@code @Save}, {@code @Update}, a {@code @Delete} that takes entities) as its primary entity if they all use the
 * same one; one method can work on another entity: a statement names it ({@code DELETE FROM Box}), a lifecycle method takes it, a query or find method returns it.
 */
final class JandexEntities {

    private static final DotName ENTITY = DotName.createSimple("de.caluga.morphium.annotations.Entity");
    private static final DotName INSERT = DotName.createSimple("jakarta.data.repository.Insert");
    private static final DotName SAVE = DotName.createSimple("jakarta.data.repository.Save");
    private static final DotName UPDATE = DotName.createSimple("jakarta.data.repository.Update");
    private static final DotName DELETE = DotName.createSimple("jakarta.data.repository.Delete");
    private static final DotName QUERY = DotName.createSimple("jakarta.data.repository.Query");

    private JandexEntities() {
    }

    static boolean isEntity(DotName type, IndexView index) {
        if (type == null) {
            return false;
        }

        ClassInfo info = index.getClassByName(type);
        return info != null && info.hasDeclaredAnnotation(ENTITY);
    }

    /** the type that a type stands for: {@code T}, {@code T[]}, {@code T...}, or the type argument of {@code List<T>}, {@code Optional<T>}, {@code Page<T>} and the like; null if there is none */
    static DotName element(Type type) {
        return switch (type.kind()) {
            case CLASS -> type.name();
            case ARRAY -> element(type.asArrayType().constituent());
            case PARAMETERIZED_TYPE -> type.asParameterizedType().arguments().size() == 1 ? element(type.asParameterizedType().arguments().get(0)) : null;
            default -> null;
        };
    }

    /** the entity of a lifecycle method, taken from its first parameter; null if the method is none */
    static DotName lifecycleEntity(MethodInfo method, IndexView index) {
        boolean annotated = method.hasAnnotation(INSERT) || method.hasAnnotation(SAVE) || method.hasAnnotation(UPDATE) || method.hasAnnotation(DELETE);

        if (!annotated || method.parametersCount() != 1) {
            return null;
        }

        DotName parameter = element(method.parameterType(0));
        return isEntity(parameter, index) ? parameter : null;
    }

    /** the kind of a lifecycle method: INSERT, SAVE, UPDATE or DELETE */
    static String lifecycleKind(MethodInfo method) {
        return method.hasAnnotation(INSERT) ? "INSERT" : method.hasAnnotation(SAVE) ? "SAVE" : method.hasAnnotation(UPDATE) ? "UPDATE" : "DELETE";
    }

    /**
     * The entity of the lifecycle methods if they all agree, otherwise null.
     */
    static DotName primaryFromLifecycle(Collection<MethodInfo> methods, IndexView index) {
        Set<DotName> entities = new LinkedHashSet<>();

        for (MethodInfo m : methods) {
            DotName entity = lifecycleEntity(m, index);

            if (entity != null) {
                entities.add(entity);
            }
        }

        return entities.size() == 1 ? entities.iterator().next() : null;
    }

    /** every entity that the methods work on, by lower-cased simple name: the primary one, the ones of lifecycle methods, and the ones that methods return */
    static Map<String, String> known(Collection<MethodInfo> methods, String primary, IndexView index) {
        Map<String, String> known = new LinkedHashMap<>();

        if (primary != null) {
            known.put(simpleName(primary).toLowerCase(Locale.ROOT), primary);
        }

        for (MethodInfo m : methods) {
            DotName lifecycle = lifecycleEntity(m, index);

            if (lifecycle != null) {
                known.putIfAbsent(simpleName(lifecycle.toString()).toLowerCase(Locale.ROOT), lifecycle.toString());
            }

            DotName returned = element(m.returnType());

            if (isEntity(returned, index)) {
                known.putIfAbsent(simpleName(returned.toString()).toLowerCase(Locale.ROOT), returned.toString());
            }
        }

        return known;
    }

    /**
     * The entity a method works on. A lifecycle method: the entity of its parameter. A JDQL statement that names its entity: that entity, which must be one of the repository. Otherwise the entity
     * that the method returns, and if it returns none, the primary entity.
     *
     * @throws IllegalStateException if a DELETE or UPDATE statement names an entity that is none of the repository; the name after FROM of a query is ignored if unknown, as it always was
     */
    static String forMethod(MethodInfo method, String primary, Map<String, String> known, IndexView index) {
        DotName lifecycle = lifecycleEntity(method, index);

        if (lifecycle != null) {
            return lifecycle.toString();
        }

        AnnotationInstance query = method.annotation(QUERY);
        String where = method.declaringClass().name() + "." + method.name();

        if (query != null && query.value() != null) {
            String jdql = query.value().asString();
            String named;

            try {
                named = JdqlEntityName.of(jdql);
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("@Query on " + where + ": " + e.getMessage(), e);
            }

            if (named != null) {
                String entity = known.get(named.toLowerCase(Locale.ROOT));

                if (entity != null) {
                    return entity;
                }

                if (JdqlDelete.is(jdql) || JdqlUpdate.is(jdql)) {
                    throw new IllegalStateException("@Query on " + where + " names the entity " + named + ", which is no entity of the repository (known: " + known.values() + ")");
                }
            }
        }

        DotName returned = element(method.returnType());
        return isEntity(returned, index) ? returned.toString() : primary;
    }

    static String simpleName(String className) {
        return className.substring(Math.max(className.lastIndexOf('.'), className.lastIndexOf('$')) + 1);
    }
}
