# Field Names Without String Literals

Avoid hard‑coded string field names in queries to prevent typos and ease refactors/migrations.

Option 1: Enums (recommended for teams)
```java
@Entity
public class User {
  @Id private MorphiumId id;
  private String userName;
  public enum Fields { id, userName }
}

var q = morphium.createQueryFor(User.class)
  .f(User.Fields.userName).eq("alice");
```

Option 2: Lambda property extractor (no codegen)

- Use a method reference to a getter; Morphium will receive the Java field name
- A small helper is included: `de.caluga.morphium.query.FieldNames` and `Property`
```java
import static de.caluga.morphium.query.FieldNames.of;

var q = morphium.createQueryFor(User.class)
  .f(of(User::getUserName)).eq("alice");
```

Notes

- Works with `@Entity(translateCamelCase = true)`—use Java field names in queries, Morphium maps to stored names.
- Requires normal JavaBean getter names (`getXxx` or `isXxx`).

Option 3: Code generation at build time (annotation processor)

- Create a simple JSR‑269 annotation processor that scans `@Entity` classes and generates `Fields` enums or `User_` constants classes using JavaPoet.
- Wire it via Maven’s `maven-compiler-plugin` `annotationProcessorPaths` or run a generator via `exec-maven-plugin` in the `generate-sources` phase to write sources into `target/generated-sources`.
- Pros: strongly typed enums/constants; Cons: extra module/maintenance.

Recommendation

- Start with Enums or the Lambda extractor. If your model is large or changes often, consider adding a lightweight codegen step later.


