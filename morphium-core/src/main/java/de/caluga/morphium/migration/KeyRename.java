package de.caluga.morphium.migration;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Renames keys inside the documents that are already stored (MOR-016): the migration that goes with a change of the key names of an {@code @Embedded} class, for example
 * when its {@code translateCamelCase} changes or the application stops writing snake_case keys under an entity that does not translate.
 *
 * <pre>{@code
 * // children: [{file_name, item_version_id}] -> [{fileName, itemVersionId}], also in the levels below
 * long changed = KeyRename.in(morphium, Owner.class).at("children").snakeToCamel().apply();
 *
 * // only the keys that are named, in a single embedded document
 * KeyRename.in(morphium, Owner.class).at("child").rename("file_name", "fileName").apply();
 * }</pre>
 *
 * The path is a stored name (dotted for a document below a document); a document or an array of documents at the path is renamed, an array on the way is walked.
 * {@code class_name} is never renamed. The documents are read, and the new value of the first segment of the path is written per document.
 * <b>A rename that would overwrite an existing key is refused with an {@link IllegalStateException} that names the document, before anything is written.</b> Run it
 * while no application instance writes the old format; it can be repeated, a second run finds nothing to do. Designed to be called from a migration, for example a
 * {@code @MorphiumChangeUnit} of the Quarkus extension.
 */
public final class KeyRename {

    private static final Set<String> KEPT = Set.of("_id", "class_name");

    private final Morphium morphium;
    private final Class<?> entity;
    private String path = "";
    private boolean snakeToCamel;
    private boolean recursive = true;
    private final Map<String, String> renames = new LinkedHashMap<>();

    private KeyRename(Morphium morphium, Class<?> entity) {
        this.morphium = morphium;
        this.entity = entity;
    }

    /** the documents of the collection of {@code entity} */
    public static KeyRename in(Morphium morphium, Class<?> entity) {
        if (morphium == null || entity == null) {
            throw new IllegalArgumentException("morphium and the entity must not be null");
        }

        return new KeyRename(morphium, entity);
    }

    /** the stored name of the document, or the array of documents, whose keys are renamed; dotted for a document below a document */
    public KeyRename at(String storedPath) {
        this.path = storedPath == null ? "" : storedPath;
        return this;
    }

    /** renames the key {@code from} to {@code to} */
    public KeyRename rename(String from, String to) {
        if (from == null || from.isEmpty() || to == null || to.isEmpty()) {
            throw new IllegalArgumentException("A rename needs two names");
        }

        renames.put(from, to);
        return this;
    }

    /** renames every key from {@code snake_case} to {@code camelCase}; combine with {@link #rename} for the exceptions, which win */
    public KeyRename snakeToCamel() {
        this.snakeToCamel = true;
        return this;
    }

    /** whether the documents below the ones at the path are renamed, too; the default is true */
    public KeyRename recursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    /**
     * Runs the rename.
     *
     * @return the number of documents that were changed
     * @throws IllegalStateException if a rename would overwrite a key
     */
    public long apply() {
        if (path.isEmpty()) {
            throw new IllegalStateException("Name the document or the array of documents whose keys are renamed: at(\"path\")");
        }

        if (!snakeToCamel && renames.isEmpty()) {
            throw new IllegalStateException("Nothing to rename: name a key with rename(from, to) or ask for snakeToCamel()");
        }

        String top = path.contains(".") ? path.substring(0, path.indexOf('.')) : path;
        String rest = path.contains(".") ? path.substring(path.indexOf('.') + 1) : "";
        List<Map<String, Object>> docs = morphium.createQueryFor(entity).f(top).exists().asMapList();
        Map<Object, Object> planned = new LinkedHashMap<>();

        // everything is planned first: a refused rename leaves the collection as it is
        for (Map<String, Object> doc : docs) {
            Object oldValue = doc.get(top);
            Object newValue = walk(oldValue, rest, doc.get("_id"));

            if (newValue != null && !newValue.equals(oldValue)) {
                planned.put(doc.get("_id"), newValue);
            }
        }

        long changed = 0;

        for (Map.Entry<Object, Object> e : planned.entrySet()) {
            Query<?> q = morphium.createQueryFor(entity).f("_id").eq(e.getKey());
            q.disableAutoValues();
            q.set(top, e.getValue());
            changed++;
        }

        return changed;
    }

    /** the value with the keys renamed at the rest of the path: null if nothing is there or nothing changed */
    @SuppressWarnings("unchecked")
    private Object walk(Object value, String rest, Object id) {
        if (value instanceof List) {
            List<Object> out = new ArrayList<>();

            for (Object element : (List<Object>) value) {
                Object r = walk(element, rest, id);
                out.add(r != null ? r : element);
            }

            return out;
        }

        if (!(value instanceof Map)) {
            return value;
        }

        Map<String, Object> doc = (Map<String, Object>) value;

        if (!rest.isEmpty()) {
            String next = rest.contains(".") ? rest.substring(0, rest.indexOf('.')) : rest;
            String remaining = rest.contains(".") ? rest.substring(rest.indexOf('.') + 1) : "";
            Map<String, Object> copy = new LinkedHashMap<>(doc);

            if (copy.containsKey(next)) {
                Object r = walk(copy.get(next), remaining, id);
                copy.put(next, r != null ? r : copy.get(next));
            }

            return copy;
        }

        return renameIn(doc, id, "");
    }

    /** a copy of the document with the keys renamed, the documents below it too if recursive */
    @SuppressWarnings("unchecked")
    private Object renameIn(Object value, Object id, String where) {
        if (value instanceof List) {
            List<Object> out = new ArrayList<>();

            for (Object element : (List<Object>) value) {
                out.add(renameIn(element, id, where));
            }

            return out;
        }

        if (!(value instanceof Map)) {
            return value;
        }

        Map<String, Object> in = (Map<String, Object>) value;
        Map<String, Object> out = new LinkedHashMap<>();

        for (Map.Entry<String, Object> e : in.entrySet()) {
            String key = e.getKey();
            String target = targetOf(key);
            Object v = recursive ? renameIn(e.getValue(), id, where + key + ".") : e.getValue();

            if (!target.equals(key) && in.containsKey(target)) {
                throw new IllegalStateException("The document " + id + " has the key " + key + " and the key " + target + " already at " + (where.isEmpty() ? "its top level" : where.substring(0, where.length() - 1))
                        + ": renaming " + key + " to " + target + " would overwrite a value. Nothing was written.");
            }

            if (out.containsKey(target)) {
                throw new IllegalStateException("The document " + id + ": " + key + " and another key are both renamed to " + target + ". Nothing was written.");
            }

            out.put(target, v);
        }

        return out;
    }

    private String targetOf(String key) {
        if (KEPT.contains(key)) {
            return key;
        }

        String explicit = renames.get(key);

        if (explicit != null) {
            return explicit;
        }

        if (snakeToCamel && key.indexOf('_') > 0) {
            return new AnnotationAndReflectionHelper(true).createCamelCase(key, false);
        }

        return key;
    }
}
