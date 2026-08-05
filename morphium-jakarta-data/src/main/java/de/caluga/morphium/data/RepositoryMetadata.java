package de.caluga.morphium.data;

/**
 * Holds the metadata extracted at build time for a single {@code @Repository} interface.
 * <p>
 * Every {@link AbstractMorphiumRepository} subclass carries exactly one instance of this record,
 * obtained from the build-time annotation processor and passed to the constructor. It is the
 * shared source of truth for the entity type used by {@link QueryExecutor},
 * {@link FindMethodBridge}, and {@link JdqlMethodBridge} when resolving field names and building
 * queries — none of them need to know how the repository interface itself is declared.
 *
 * @param entityClass the entity type {@code T}
 * @param idClass     the primary-key type {@code K}
 * @param idFieldName the Java field name annotated with {@code @Id}
 */
public record RepositoryMetadata(
        Class<?> entityClass,
        Class<?> idClass,
        String idFieldName
) {}
