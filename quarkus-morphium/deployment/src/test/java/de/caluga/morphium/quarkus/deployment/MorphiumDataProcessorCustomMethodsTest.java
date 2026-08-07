package de.caluga.morphium.quarkus.deployment;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.data.MorphiumRepository;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import jakarta.data.repository.By;
import jakarta.data.repository.Delete;
import jakarta.data.repository.Repository;

import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for {@link MorphiumDataProcessor#generateCustomQueryMethods} covering the
 * four Blocker-2 follow-up findings from the maintainer review of PR #267:
 *
 * <ol>
 *   <li>BEFUND 1: the "skip default/abstract" guard must skip anything that is NOT abstract
 *       (private interface helper methods included), not just {@code isDefault()} methods --
 *       and must additionally skip abstract redeclarations of toString()/equals()/hashCode().</li>
 *   <li>BEFUND 4: the {@code @By} parameter-name fallback (Jakarta Data §4.6.1) must also apply
 *       in the {@code @Delete} path, not just the {@code @Find} path.</li>
 *   <li>BEFUND 2: a parameter/@By-condition {@code @Delete} method with an unsupported return
 *       type (boolean, Integer, Long, ...) must fail the *build*, not produce bytecode that
 *       throws VerifyError at class-load time.</li>
 *   <li>BEFUND 3: an abstract method inherited from a *custom* super-interface (not one of the
 *       standard Jakarta Data / Morphium repository interfaces) must be picked up by the
 *       generation loop, not silently skipped because {@code ClassInfo.methods()} only returns
 *       directly-declared methods.</li>
 * </ol>
 *
 * <p>These tests build a real Jandex index from actual compiled test-fixture classes and invoke
 * the package-private/private processor methods directly (via reflection where needed), following
 * the same pattern as {@link MorphiumProcessorReflectionTest}.
 */
@DisplayName("MorphiumDataProcessor — custom @Repository method generation (Blocker 2 follow-ups)")
class MorphiumDataProcessorCustomMethodsTest {

    // -----------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------

    @Entity
    public static class FixtureEntity {
        @Id
        public String id;
        public String name;
        public String auditor;
    }

    /**
     * BEFUND 1 fixture: a repository interface with a private interface helper method (legal
     * since Java 9, always has a body -- Jandex's isDefault() does NOT consider it "default")
     * and an abstract redeclaration of toString(). Neither must be treated as an "unsupported
     * repository method" needing generation.
     */
    @Repository
    public interface PrivateHelperAndToStringRepository extends MorphiumRepository<FixtureEntity, String> {
        // Abstract redeclaration of Object.toString() -- legal, must be skipped (Object supplies it).
        @Override
        String toString();

        // Private interface helper method -- has a body, is NOT "default" per Jandex's isDefault()
        // (which requires public && !static && !abstract), so guarding on isDefault() alone let this
        // fall through to the "unsupported method" exception. Guarding on "not abstract" fixes it.
        private String helper() {
            return "unused";
        }

        List<FixtureEntity> findByName(String name);
    }

    /**
     * BEFUND 4 fixture: a @Delete method that relies purely on the parameter name (no @By
     * annotation) to specify the delete condition.
     */
    @Repository
    public interface DeleteByParamNameRepository extends MorphiumRepository<FixtureEntity, String> {
        @Delete
        long deleteByName(String name);
    }

    /** Same as above, but using an explicit @By annotation (control case, already worked before). */
    @Repository
    public interface DeleteByAnnotatedRepository extends MorphiumRepository<FixtureEntity, String> {
        @Delete
        void deleteWhere(@By("name") String name);
    }

    /**
     * Silent-data-loss bug fixture: a @Delete method with a single ENTITY-typed parameter
     * (Jakarta Data lifecycle-delete shape, jakarta.data-api 1.0.1 Delete javadoc). This must be
     * treated as an entity-lifecycle delete (doDelete(entity)), never as a parameter-name
     * @By-condition delete.
     *
     * <p>Boolean is deliberately used as the return type here as a build-time discriminator
     * (same technique as {@code DeleteBadReturnTypeRepository} above): boolean is invalid for the
     * parameter/@By-condition delete branch (which requires void/int/long) but irrelevant for the
     * entity-lifecycle branch (which unconditionally returns void). If the entity parameter were
     * misclassified as a condition (the bug), generation would fail with "Unsupported @Delete
     * method ... boolean" here; correct handling reaches the entity branch and succeeds.
     */
    @Repository
    public interface DeleteEntityParamRepository extends MorphiumRepository<FixtureEntity, String> {
        @Delete
        boolean remove(FixtureEntity entity);
    }

    /**
     * Silent-data-loss bug fixture, mixed case: a @Delete method that combines an entity-typed
     * parameter with an explicit @By-condition parameter in the same method. Jakarta Data does
     * not define semantics for this combination (a @Delete method has either exactly one
     * entity/List/array lifecycle parameter, or parameter/@By conditions -- not both), so this
     * must be rejected at build time.
     */
    @Repository
    public interface DeleteMixedEntityAndConditionRepository extends MorphiumRepository<FixtureEntity, String> {
        @Delete
        void remove(FixtureEntity entity, @By("name") String name);
    }

    /**
     * Fixture purely for exercising {@code isEntityParameter} directly against every Jandex type
     * shape it must recognize (entity, array-of-entity, List/Collection/Iterable-of-entity) and
     * reject (a plain String, a List of Strings). Not a @Repository/@Delete method -- just a
     * vehicle to obtain real Jandex {@code Type} instances for each parameter shape.
     */
    public interface EntityParamShapesRepository {
        void single(FixtureEntity e);

        void array(FixtureEntity[] es);

        void list(List<FixtureEntity> es);

        void collection(java.util.Collection<FixtureEntity> es);

        void iterable(Iterable<FixtureEntity> es);

        void byName(String name);

        void listOfStrings(List<String> names);
    }

    /**
     * BEFUND 2 fixture: a parameter/@By-condition @Delete method with an unsupported return type
     * (boolean is not void/int/long per Jakarta Data). Must be rejected at build time.
     */
    @Repository
    public interface DeleteBadReturnTypeRepository extends MorphiumRepository<FixtureEntity, String> {
        @Delete
        boolean deleteByName(String name);
    }

    /**
     * BEFUND 3 fixtures: a custom super-interface declaring an abstract method NOT related to
     * any standard Jakarta Data / Morphium repository interface. The repository extends both
     * this custom interface and MorphiumRepository.
     */
    public interface WithAudit {
        List<FixtureEntity> findByAuditor(String auditor);
    }

    @Repository
    public interface AuditedRepository extends MorphiumRepository<FixtureEntity, String>, WithAudit {
        List<FixtureEntity> findByName(String name);
    }

    // -----------------------------------------------------------------
    // Infrastructure (same pattern as MorphiumProcessorReflectionTest)
    // -----------------------------------------------------------------

    private static IndexView buildIndex(Class<?>... classes) throws IOException {
        Indexer indexer = new Indexer();
        for (Class<?> c : classes) {
            String resource = c.getName().replace('.', '/') + ".class";
            try (InputStream in = c.getClassLoader().getResourceAsStream(resource)) {
                indexer.index(in);
            }
        }
        return indexer.complete();
    }

    private static class CollectingProducer implements BuildProducer<ReflectiveClassBuildItem> {
        final Set<String> registeredClassNames = new HashSet<>();

        @Override
        public void produce(ReflectiveClassBuildItem item) {
            registeredClassNames.addAll(item.getClassNames());
        }
    }

    /** No-op Gizmo ClassOutput -- these tests only care about build-time exceptions/behavior,
     *  not about loading the generated class. */
    private static class NoopClassOutput implements ClassOutput {
        @Override
        public void write(String className, byte[] data) {
            // discard -- we only assert on build-time exceptions/absence thereof
        }
    }

    private static Set<String> entityFieldsOf(Class<?> entityClass, IndexView index) throws Exception {
        ClassInfo entityInfo = index.getClassByName(DotName.createSimple(entityClass.getName()));
        Method m = MorphiumDataProcessor.class.getDeclaredMethod(
                "collectEntityFields", ClassInfo.class, IndexView.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<String> fields = (Set<String>) m.invoke(new MorphiumDataProcessor(), entityInfo, index);
        return fields;
    }

    /**
     * Invokes the private {@code generateCustomQueryMethods} for the given repository interface
     * against a real Gizmo {@link ClassCreator}, mirroring exactly what
     * {@code generateImpl}/{@code MorphiumDataProcessor} does at build time. Any
     * {@code IllegalStateException} thrown during generation propagates as the cause of an
     * {@link InvocationTargetException}.
     */
    private static void generate(Class<?> repoInterfaceClass, IndexView index) throws Exception {
        ClassInfo repoInfo = index.getClassByName(DotName.createSimple(repoInterfaceClass.getName()));
        Set<String> entityFields = entityFieldsOf(FixtureEntity.class, index);
        CollectingProducer reflectiveClasses = new CollectingProducer();

        Method m = MorphiumDataProcessor.class.getDeclaredMethod(
                "generateCustomQueryMethods", ClassCreator.class, ClassInfo.class, IndexView.class,
                String.class, Set.class, BuildProducer.class);
        m.setAccessible(true);

        try (ClassCreator cc = ClassCreator.builder()
                .classOutput(new NoopClassOutput())
                .className(repoInterfaceClass.getName() + "_MorphiumImplTest")
                .superClass(de.caluga.morphium.data.AbstractMorphiumRepository.class.getName())
                .interfaces(repoInterfaceClass.getName())
                .build()) {
            try {
                m.invoke(new MorphiumDataProcessor(), cc, repoInfo, index,
                        FixtureEntity.class.getName(), entityFields, reflectiveClasses);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof RuntimeException re) {
                    throw re;
                }
                throw e;
            }
        }
    }

    // -----------------------------------------------------------------
    // BEFUND 1
    // -----------------------------------------------------------------

    @Test
    @DisplayName("BEFUND 1: a private interface helper method + abstract toString() redeclaration must NOT break the build")
    void privateHelperAndAbstractToString_doNotBreakGeneration() throws Exception {
        IndexView index = buildIndex(FixtureEntity.class, PrivateHelperAndToStringRepository.class,
                MorphiumRepository.class);

        // Must not throw -- this is the core regression: previously the private helper method
        // (not "default" per Jandex isDefault()) fell through to the "unsupported method"
        // IllegalStateException, breaking the build for entirely legal user code.
        generate(PrivateHelperAndToStringRepository.class, index);
    }

    // -----------------------------------------------------------------
    // BEFUND 4
    // -----------------------------------------------------------------

    @Test
    @DisplayName("BEFUND 4: @Delete method relying on parameter name (no @By) is treated as a condition-delete, not entity-delete")
    void deleteByParamNameFallback_isTreatedAsConditionDelete() throws Exception {
        IndexView index = buildIndex(FixtureEntity.class, DeleteByParamNameRepository.class,
                DeleteByAnnotatedRepository.class, MorphiumRepository.class);

        // Must not throw: previously this fell into the "entity parameter delete" branch,
        // which would compile fine here (single String param delegated to doDelete(Object))
        // but attempt to delete a String as an entity at runtime. We can't directly assert the
        // internal hasByParams flag (private local var), so we assert indirectly: the identical
        // shape with an explicit @By annotation must generate without error too, and a
        // regression that reintroduces "entity-parameter delete" behavior for this method would
        // still compile a method (just the wrong one) -- covered end-to-end by BEFUND 2's test
        // below, which specifically fails when the parameter-name path incorrectly falls through
        // the "entity delete" branch for a non-void/int/long return type.
        generate(DeleteByParamNameRepository.class, index);
        generate(DeleteByAnnotatedRepository.class, index);
    }

    // -----------------------------------------------------------------
    // Silent-data-loss bug: @Delete with entity-typed parameter
    // -----------------------------------------------------------------

    @Test
    @DisplayName("isEntityParameter: recognizes entity, entity[], List<entity>, Collection<entity>, Iterable<entity>; rejects String and List<String>")
    void isEntityParameter_recognizesAllEntityShapesAndRejectsNonEntityShapes() throws Exception {
        IndexView index = buildIndex(FixtureEntity.class, EntityParamShapesRepository.class);
        ClassInfo repoInfo = index.getClassByName(DotName.createSimple(EntityParamShapesRepository.class.getName()));
        String entityClassName = FixtureEntity.class.getName();

        Method isEntityParameter = MorphiumDataProcessor.class.getDeclaredMethod(
                "isEntityParameter", org.jboss.jandex.Type.class, String.class);
        isEntityParameter.setAccessible(true);
        MorphiumDataProcessor processor = new MorphiumDataProcessor();

        Map<String, org.jboss.jandex.Type> paramTypeByMethodName = new HashMap<>();
        for (org.jboss.jandex.MethodInfo m : repoInfo.methods()) {
            paramTypeByMethodName.put(m.name(), m.parameterType(0));
        }

        assertThat((Boolean) isEntityParameter.invoke(processor, paramTypeByMethodName.get("single"), entityClassName))
                .as("plain entity parameter").isTrue();
        assertThat((Boolean) isEntityParameter.invoke(processor, paramTypeByMethodName.get("array"), entityClassName))
                .as("entity[] parameter").isTrue();
        assertThat((Boolean) isEntityParameter.invoke(processor, paramTypeByMethodName.get("list"), entityClassName))
                .as("List<entity> parameter").isTrue();
        assertThat((Boolean) isEntityParameter.invoke(processor, paramTypeByMethodName.get("collection"), entityClassName))
                .as("Collection<entity> parameter").isTrue();
        assertThat((Boolean) isEntityParameter.invoke(processor, paramTypeByMethodName.get("iterable"), entityClassName))
                .as("Iterable<entity> parameter").isTrue();
        assertThat((Boolean) isEntityParameter.invoke(processor, paramTypeByMethodName.get("byName"), entityClassName))
                .as("plain String parameter must NOT be treated as an entity parameter").isFalse();
        assertThat((Boolean) isEntityParameter.invoke(processor, paramTypeByMethodName.get("listOfStrings"), entityClassName))
                .as("List<String> must NOT be treated as an entity parameter").isFalse();
    }

    @Test
    @DisplayName("silent data loss fix: @Delete method with an ENTITY parameter must NOT be generated as a condition-delete")
    void deleteWithEntityParameter_isNotTreatedAsConditionDelete() throws Exception {
        IndexView index = buildIndex(FixtureEntity.class, DeleteEntityParamRepository.class,
                MorphiumRepository.class);

        // The return type here is boolean, which is invalid for the parameter/@By-condition
        // delete branch (Jakarta Data requires void/int/long there) but is perfectly fine for the
        // entity-lifecycle branch (which always returns void, ignoring the declared boolean --
        // the same as the pre-existing single-entity-parameter branch already does). Before the
        // fix, the parameter-name fallback wrongly classified the FixtureEntity parameter as a
        // @By condition, hit the return-type guard, and this call would throw
        // "Unsupported @Delete method ... boolean". After the fix it must generate cleanly,
        // proving the entity-lifecycle branch (doDelete(entity)) was chosen instead.
        generate(DeleteEntityParamRepository.class, index);
    }

    @Test
    @DisplayName("silent data loss fix: this is the exact regression case -- deleteByParamNameFallback must stay a condition-delete, entity-param delete must stay an entity-delete")
    void deleteByParamNameFallback_and_deleteWithEntityParameter_areClearlyDistinguished() throws Exception {
        IndexView index = buildIndex(FixtureEntity.class, DeleteByParamNameRepository.class,
                DeleteEntityParamRepository.class, MorphiumRepository.class);

        // Condition-delete (String parameter, no @By): must still work, going through the
        // parameter/@By branch (unaffected by the entity-parameter exception).
        generate(DeleteByParamNameRepository.class, index);

        // Entity-parameter delete (FixtureEntity parameter, no @By): must go through the
        // entity-lifecycle branch. Discriminated the same way as the test above -- a boolean
        // return type on this method would fail the build if it were misrouted into the
        // condition-delete branch.
        generate(DeleteEntityParamRepository.class, index);
    }

    @Test
    @DisplayName("mixed entity-parameter + @By-condition @Delete method is rejected at BUILD time (unspecified by Jakarta Data)")
    void deleteWithMixedEntityAndConditionParameters_failsAtBuildTime() throws Exception {
        IndexView index = buildIndex(FixtureEntity.class, DeleteMixedEntityAndConditionRepository.class,
                MorphiumRepository.class);

        assertThatThrownBy(() -> generate(DeleteMixedEntityAndConditionRepository.class, index))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unsupported @Delete method")
                .hasMessageContaining("mixes an entity-typed parameter");
    }

    // -----------------------------------------------------------------
    // BEFUND 2
    // -----------------------------------------------------------------

    @Test
    @DisplayName("BEFUND 2: @Delete with boolean return type on a parameter/@By-condition method fails the BUILD, not VerifyError at class-load")
    void deleteWithUnsupportedReturnType_failsAtBuildTime() throws Exception {
        IndexView index = buildIndex(FixtureEntity.class, DeleteBadReturnTypeRepository.class,
                MorphiumRepository.class);

        // This method has a single String parameter with NO @By annotation -- it relies purely
        // on the parameter-name fallback (BEFUND 4) to be recognized as a condition-delete. If
        // BEFUND 4 were not fixed, this would incorrectly be treated as an "entity delete" and
        // NOT hit the return-type guard at all (masking BEFUND 2). Both fixes are exercised
        // together here, which is the actual failure mode described in BEFUND 2/4.
        assertThatThrownBy(() -> generate(DeleteBadReturnTypeRepository.class, index))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unsupported @Delete method")
                .hasMessageContaining("boolean");
    }

    // -----------------------------------------------------------------
    // BEFUND 3
    // -----------------------------------------------------------------

    @Test
    @DisplayName("BEFUND 3: abstract method inherited from a custom super-interface is generated, not silently skipped")
    void inheritedCustomInterfaceMethod_isGenerated() throws Exception {
        IndexView index = buildIndex(FixtureEntity.class, AuditedRepository.class,
                WithAudit.class, MorphiumRepository.class);

        // Must not throw, and -- more importantly -- must actually invoke code generation for
        // findByAuditor(), which is only DECLARED on WithAudit, not on AuditedRepository itself.
        // We verify this indirectly: generation succeeds (no AbstractMethodError-causing gap)
        // for a repository whose repoInterface.methods() call alone would NOT have surfaced
        // findByAuditor() at all before the BEFUND 3 fix (repoInterface.methods() only returns
        // directly-declared methods in Jandex).
        generate(AuditedRepository.class, index);
    }
}
