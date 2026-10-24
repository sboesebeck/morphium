# Migrating the key names inside embedded documents

When an `@Embedded` class changes the names it stores (its `translateCamelCase` flag changes, or the application stops writing
`file_name` under an entity with `translateCamelCase=false`, see [update pitfalls](update-pitfalls.md) section 3), the documents that
are already stored keep the old names, and the new code does not find them. `KeyRename` (MOR-016) renames the keys in the stored
documents.

```java
// children: [{file_name, item_version_id, inner_grand_child: {deep_value}}] -> [{fileName, itemVersionId, innerGrandChild: {deepValue}}]
long changed = KeyRename.in(morphium, Owner.class).at("children").snakeToCamel().apply();

// a single embedded document, and only the keys that are named
KeyRename.in(morphium, Owner.class).at("child").rename("file_name", "fileName").apply();
```

- `at(path)` is the **stored** name of the embedded document or of the array of embedded documents (dotted for a document below a document).
  An array on the way is walked.
- `snakeToCamel()` renames every key below the path, also in the levels below (`recursive(false)` limits it to the documents at the path).
  `rename(from, to)` names single keys and wins over `snakeToCamel()`. `_id` and `class_name` are never renamed.
- A rename that would overwrite a key that is already there (`file_name` next to `fileName`) throws an `IllegalStateException` that names
  the document. The renames are planned for the whole collection before the first write, so nothing is written then.
- The result is the number of documents that changed; a second run finds nothing to do. Run it while no instance of the application writes the old format.

As a change unit of the Quarkus extension, it runs once, in order, with the lock of the migration runner:

```java
@MorphiumChangeUnit(id = "003-camel-case-children", order = "003", author = "team")
public class CamelCaseChildren {

    @Execution
    public void execute(Morphium morphium) {
        KeyRename.in(morphium, Owner.class).at("children").snakeToCamel().apply();
        KeyRename.in(morphium, Owner.class).at("child").snakeToCamel().apply();
    }
}
```

The entries of an embedded **list** carry `class_name`. It is the name of the class that wrote the element and stays as it is: the class is the same one, with the new annotation.

Tests: `KeyRenameTest` (an embedded list and a single embedded document, explicit renames, the refused overwrite) on the InMemoryDriver and on MongoDB 8.
The change unit above was not run as a change unit; the helper it calls was.
