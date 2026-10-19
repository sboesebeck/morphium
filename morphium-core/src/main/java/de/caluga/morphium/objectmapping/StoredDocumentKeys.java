package de.caluga.morphium.objectmapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A helper for tests: finds the snake_case keys of a stored document. A key that contains an underscore shows that a class was stored with camel case translation (file_name for
 * fileName); {@code _id} and {@code class_name} are written by Morphium itself and are not reported. Use it on a raw document to assert that a class that is meant to keep its
 * camelCase names did so, for example after the warning of {@code ObjectMappingSettings.strictEmbeddedNaming} (MOR-014):
 * <pre>{@code
 * Map<String, Object> raw = query.asMapList().get(0);
 * assertTrue(StoredDocumentKeys.snakeCaseKeys(raw).isEmpty());
 * }</pre>
 */
public final class StoredDocumentKeys {
    private StoredDocumentKeys() {
    }

    /** the paths of the keys that contain an underscore, other than {@code _id} and {@code class_name}; documents in lists are addressed by their index ({@code children.1.item_id}) */
    public static List<String> snakeCaseKeys(Map<String, Object> document) {
        List<String> found = new ArrayList<>();

        if (document != null) {
            walk("", document, found);
        }

        return found;
    }

    private static void walk(String prefix, Object value, List<String> found) {
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                String key = String.valueOf(e.getKey());
                String path = prefix.isEmpty() ? key : prefix + "." + key;

                if (key.indexOf('_') >= 0 && !key.equals("_id") && !key.equals("class_name")) {
                    found.add(path);
                }

                walk(path, e.getValue(), found);
            }
        } else if (value instanceof Collection<?> list) {
            int i = 0;

            for (Object member : list) {
                walk(prefix + "." + i++, member, found);
            }
        }
    }
}
