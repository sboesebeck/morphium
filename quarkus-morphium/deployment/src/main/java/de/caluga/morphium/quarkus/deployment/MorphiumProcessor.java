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

import de.caluga.morphium.annotations.Capped;
import de.caluga.morphium.annotations.Driver;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Messaging;
import de.caluga.morphium.quarkus.MorphiumRecorder;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationEntry;
import de.caluga.morphium.quarkus.migration.MorphiumMigrationLock;
import de.caluga.morphium.DefaultNameProvider;
import de.caluga.morphium.encryption.DefaultEncryptionKeyProvider;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.morphium.IndexDescription;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedPackageBuildItem;
import io.quarkus.smallrye.health.deployment.spi.HealthBuildItem;
import de.caluga.morphium.quarkus.MorphiumProducer;
import de.caluga.morphium.quarkus.transaction.MorphiumTransactionalInterceptor;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.DotName;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.IndexView;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Quarkus build-time processor for the Morphium extension.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>Register the {@code "morphium"} feature so it appears in the Quarkus banner.</li>
 *   <li>Make the CDI producer bean available to the application.</li>
 *   <li>Register all classes annotated with {@link Entity} or {@link Embedded}
 *       for GraalVM reflection so that Morphium's ObjectMapper can serialise and
 *       deserialise them in a native image without requiring {@code reflect-config.json}.</li>
 * </ol>
 *
 * <p>This class uses only standard Quarkus build-item APIs and Jandex for
 * annotation scanning – no {@code sun.*} imports, no {@code Unsafe} access.
 *
 * <p><b>Note:</b> Jandex only discovers {@code @Entity}/{@code @Embedded} classes in
 * the application and in dependencies that provide a Jandex index. For entities in
 * external (unindexed) JARs, add {@code quarkus.index-dependency} entries in
 * {@code application.properties} or use the {@code jandex-maven-plugin}.
 */
public class MorphiumProcessor {

    private static final Logger log = Logger.getLogger(MorphiumProcessor.class);

    // ------------------------------------------------------------------
    // Feature registration
    // ------------------------------------------------------------------

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(MorphiumFeature.FEATURE_NAME);
    }

    // ------------------------------------------------------------------
    // Jandex index for morphium-core (ships no jandex.idx in its JAR)
    // ------------------------------------------------------------------

    /**
     * Instructs Quarkus to index the morphium-core JAR so that Jandex
     * picks up {@code @Driver}, {@code @Messaging}, {@code @Entity},
     * {@code @Embedded}, and {@code @Capped} classes from morphium-core
     * itself. Without this, {@code CombinedIndexBuildItem} only contains
     * application classes, and the driver/messaging discovery silently
     * returns empty lists.
     */
    @BuildStep
    IndexDependencyBuildItem indexMorphiumCore() {
        return new IndexDependencyBuildItem("de.caluga", "morphium");
    }

    // ------------------------------------------------------------------
    // CDI bean registration
    // ------------------------------------------------------------------

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        // Register runtime CDI beans required by the extension.
        // MorphiumRuntimeConfig / CacheConfig are @ConfigMapping interfaces and are
        // registered automatically by the SmallRye Config Quarkus extension.
        // MorphiumRecorder is a @Recorder (build-time only) and must not appear here.
        // MorphiumBlockingCallDetector is no longer a CDI bean: it is a plain static
        // utility invoked directly by MorphiumProducer.buildMorphium() right after the
        // real connect, so it must not be registered here.
        return AdditionalBeanBuildItem.builder()
            .addBeanClasses(
                MorphiumProducer.class,
                MorphiumTransactionalInterceptor.class)
            .setUnremovable()
            .build();
    }

    // ------------------------------------------------------------------
    // JSON serialization: MorphiumId <-> hex string
    // ------------------------------------------------------------------

    /**
     * Registers the {@code MorphiumId} JSON customizers, but only for the JSON
     * layer(s) actually present on the application classpath.
     *
     * <p>Without these, Jackson/JSON-B walk {@code MorphiumId}'s getters and emit
     * the internal {@code {pid, counter, machineId, bytes, time}} struct, which is
     * unusable as a row id on the consumer side (a frontend grid keying rows by id
     * gets {@code "[object Object]"} for every row). The customizers serialize
     * {@code MorphiumId} as its canonical 24-char hex string and parse it back.
     *
     * <p>Gating on {@link Capabilities} keeps {@code quarkus-jackson} /
     * {@code quarkus-jsonb} optional: the customizer beans are added only when the
     * matching capability is registered, so an app that pulls in neither JSON layer
     * never references the (absent) customizer classes.
     */
    @BuildStep
    void registerMorphiumIdJsonCustomizers(Capabilities capabilities,
                                           BuildProducer<AdditionalBeanBuildItem> additionalBeans) {
        if (capabilities.isPresent(Capability.JACKSON)) {
            additionalBeans.produce(AdditionalBeanBuildItem.builder()
                .addBeanClass("de.caluga.morphium.quarkus.json.MorphiumIdJacksonModule")
                .setUnremovable()
                .build());
        }
        if (capabilities.isPresent(Capability.JSONB)) {
            additionalBeans.produce(AdditionalBeanBuildItem.builder()
                .addBeanClass("de.caluga.morphium.quarkus.json.MorphiumIdJsonbModule")
                .setUnremovable()
                .build());
        }
    }

    // ------------------------------------------------------------------
    // Micrometer metrics: connection-pool / driver-stats gauges (MVP)
    // ------------------------------------------------------------------

    /**
     * Registers {@code MorphiumMetricsBinder} as a CDI bean, but only when the application
     * has Micrometer on its classpath (Capability.METRICS present at build time).
     *
     * <p>Mirrors {@link #registerMorphiumIdJsonCustomizers}'s exact structure: gating on
     * {@link Capabilities}, producing an {@link AdditionalBeanBuildItem} referenced by class
     * name (so this processor class never needs a compile-time dependency on Micrometer types
     * itself), and marking it unremovable. Apps without Micrometer never get this bean added,
     * so {@code MorphiumMetricsBinder} — which does have a hard compile-time dependency on
     * Micrometer's {@code MeterRegistry} — never loads on their classpath at all.
     *
     * <p>Scope note: this build step only registers the Phase 1 (MVP) gauge binder. The
     * Counter/Timer sources from {@code MorphiumStorageListener}/{@code MorphiumTransactionEvent}
     * (Section 5 catalog rows {@code morphium.operations.*}/{@code morphium.transactions.*}) are
     * explicitly deferred to a later phase and are not registered here.
     */
    @BuildStep
    void registerObservability(Capabilities capabilities,
                               BuildProducer<AdditionalBeanBuildItem> additionalBeans) {
        if (capabilities.isPresent(Capability.METRICS)) {
            additionalBeans.produce(AdditionalBeanBuildItem.builder()
                .addBeanClass("de.caluga.morphium.quarkus.observability.MorphiumMetricsBinder")
                .setUnremovable()
                .build());
        }
    }

    // ------------------------------------------------------------------
    // Health check registration
    // ------------------------------------------------------------------

    @BuildStep
    HealthBuildItem addLivenessCheck(MorphiumHealthBuildTimeConfig config) {
        return new HealthBuildItem(
                "de.caluga.morphium.quarkus.health.MorphiumLivenessCheck",
                config.enabled());
    }

    @BuildStep
    HealthBuildItem addReadinessCheck(MorphiumHealthBuildTimeConfig config) {
        return new HealthBuildItem(
                "de.caluga.morphium.quarkus.health.MorphiumReadinessCheck",
                config.enabled());
    }

    @BuildStep
    HealthBuildItem addStartupCheck(MorphiumHealthBuildTimeConfig config) {
        return new HealthBuildItem(
                "de.caluga.morphium.quarkus.health.MorphiumStartupCheck",
                config.enabled());
    }

    // ------------------------------------------------------------------
    // GraalVM native image: reflection registration for @Entity / @Embedded
    // ------------------------------------------------------------------

    /**
     * Registers Morphium entity class names for GraalVM reflection and stores them in the
     * {@link MorphiumRecorder} for later use during runtime initialization.
     *
     * <p><b>Why {@code RUNTIME_INIT}:</b> This step writes into a {@code static volatile}
     * field in {@code MorphiumRecorder}. It must complete <em>before</em> all
     * subsequent {@code RUNTIME_INIT} steps — in particular before
     * {@link MorphiumMigrationProcessor#executeMigrations}, which triggers the first CDI lookup
     * of {@code Morphium} and therefore calls {@code ensureIndicesFor()} on the entity list.
     * An earlier draft used {@code STATIC_INIT} here, but that caused a race condition: the
     * entity list could still be empty when {@code Morphium} was first instantiated, resulting
     * in missing unique indexes and silent {@code saveDuplicate} test failures.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    MorphiumEntitiesRegisteredBuildItem registerEntitiesForReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                                       CombinedIndexBuildItem combinedIndex,
                                       MorphiumRecorder recorder) {
        // Collect @Entity and @Embedded class names separately for ClassGraphCache
        // pre-registration, plus a combined set for typeId/index registration.
        Set<String> allClassNames = new LinkedHashSet<>();
        List<String> entityClassNames = new ArrayList<>();
        List<String> embeddedClassNames = new ArrayList<>();
        IndexView index = combinedIndex.getIndex();

        DotName entityDotName = DotName.createSimple(Entity.class.getName());
        DotName embeddedDotName = DotName.createSimple(Embedded.class.getName());

        // Track already-registered superclasses to avoid duplicates
        Set<String> registeredSuperclasses = new LinkedHashSet<>();

        for (AnnotationInstance ai : index.getAnnotations(entityDotName)) {
            if (ai.target().kind() == AnnotationTarget.Kind.CLASS) {
                String className = ai.target().asClass().name().toString();
                registerClass(className, reflectiveClasses);
                registerSuperclasses(ai.target().asClass(), index, reflectiveClasses, registeredSuperclasses);
                registerSubclasses(ai.target().asClass(), index, reflectiveClasses, registeredSuperclasses);
                entityClassNames.add(className);
                allClassNames.add(className);
                registerCustomNameProvider(ai, reflectiveClasses);
            }
        }
        for (AnnotationInstance ai : index.getAnnotations(embeddedDotName)) {
            if (ai.target().kind() == AnnotationTarget.Kind.CLASS) {
                String className = ai.target().asClass().name().toString();
                registerClass(className, reflectiveClasses);
                registerSuperclasses(ai.target().asClass(), index, reflectiveClasses, registeredSuperclasses);
                registerSubclasses(ai.target().asClass(), index, reflectiveClasses, registeredSuperclasses);
                embeddedClassNames.add(className);
                // @Embedded classes need pre-registration for typeId mapping
                allClassNames.add(className);
            }
        }

        // Extension-internal @Entity classes are not in the app Jandex index — register for
        // reflection only. They are NOT added to mappedClassNames because their collections may
        // be renamed via configuration, and ensureIndicesFor() would create indexes on the
        // annotation-defined names instead of the configured ones.
        registerClass(MorphiumMigrationEntry.class.getName(), reflectiveClasses);
        registerClass(MorphiumMigrationLock.class.getName(), reflectiveClasses);

        // Morphium-internal classes reflectively instantiated via getDeclaredConstructor().newInstance():
        // - DefaultNameProvider: ObjectMapperImpl.getNameProviderForClass(), default @Entity(nameProvider=...)
        // - DefaultEncryptionKeyProvider: Morphium.initializeAndConnect(), default encryption key provider
        // - AESEncryptionProvider: Morphium.initializeAndConnect(), default value encryption provider
        registerClass(DefaultNameProvider.class.getName(), reflectiveClasses);
        registerClass(DefaultEncryptionKeyProvider.class.getName(), reflectiveClasses);
        registerClass(AESEncryptionProvider.class.getName(), reflectiveClasses);

        // IndexDescription: uses AnnotationAndReflectionHelper.getField() and getAllFields() for
        // reflective field access in fromMap() and asMap(). Without registration, getDeclaredFields()
        // returns no fields in native mode, so IndexDescription.key is never populated → NPE in createIndex().
        registerClass(IndexDescription.class.getName(), reflectiveClasses);

        // HelloResult: fromMsg() and toMsg() use getAllFields(HelloResult.class) to parse the MongoDB
        // hello/isMaster response via reflection. Without registration, critical fields like setName,
        // isWritablePrimary, hosts are silently null → driver cannot detect replica sets.
        registerClass("de.caluga.morphium.driver.wire.HelloResult", reflectiveClasses);

        // Wire protocol message classes — reflectively instantiated via
        // WireProtocolMessage.OpCode.handler.getDeclaredConstructor().newInstance() when
        // parsing MongoDB server responses. All 9 OpCode handler classes need registration.
        String[] wireProtocolClasses = {
            "de.caluga.morphium.driver.wireprotocol.OpReply",
            "de.caluga.morphium.driver.wireprotocol.OpUpdate",
            "de.caluga.morphium.driver.wireprotocol.OpInsert",
            "de.caluga.morphium.driver.wireprotocol.OpQuery",
            "de.caluga.morphium.driver.wireprotocol.OpGetMore",
            "de.caluga.morphium.driver.wireprotocol.OpDelete",
            "de.caluga.morphium.driver.wireprotocol.OpKillCursors",
            "de.caluga.morphium.driver.wireprotocol.OpCompressed",
            "de.caluga.morphium.driver.wireprotocol.OpMsg"
        };
        for (String cls : wireProtocolClasses) {
            registerClass(cls, reflectiveClasses);
        }

        // Pass discovered @Entity/@Embedded classes to runtime for registerTypeIds() pre-registration.
        // This combined list is used ONLY for typeId mapping (buildTypeIdMap) — NOT for index creation.
        // ensureIndices() must use getEntityClassNames() because ensureIndicesFor() calls
        // getCollectionName() which throws IllegalArgumentException for @Embedded-only classes.
        // Always call setMappedClassNames (even when empty) to reset state on hot reload.
        if (!allClassNames.isEmpty()) {
            log.infof("Morphium: passing %d @Entity/@Embedded classes for runtime pre-registration", allClassNames.size());
        }
        recorder.setMappedClassNames(new ArrayList<>(allClassNames));

        // Pass @Entity and @Embedded lists separately for ClassGraphCache pre-population.
        // In native mode, ObjectMapperImpl calls getClassesWithAnnotation(Entity.class.getName())
        // which must find the pre-populated cache entry to avoid a live ClassGraph scan.
        recorder.setEntityClassNames(entityClassNames);
        recorder.setEmbeddedClassNames(embeddedClassNames);
        return new MorphiumEntitiesRegisteredBuildItem();
    }

    // ------------------------------------------------------------------
    // GraalVM native image: @Driver class discovery and pre-population
    // ------------------------------------------------------------------

    /**
     * Discovers all {@code @Driver}-annotated classes at build time via Jandex,
     * registers them for GraalVM reflection, and passes the list to the recorder
     * so that {@link MorphiumProducer} can pre-populate {@code ClassGraphCache}
     * before {@code Morphium} is constructed. This bypasses the ClassGraph
     * classpath scan that fails in native mode.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void registerDriversForNative(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                                  CombinedIndexBuildItem combinedIndex,
                                  MorphiumRecorder recorder) {
        IndexView index = combinedIndex.getIndex();
        DotName driverDotName = DotName.createSimple(Driver.class.getName());
        List<String> driverNames = new ArrayList<>();

        for (AnnotationInstance ai : index.getAnnotations(driverDotName)) {
            if (ai.target().kind() == AnnotationTarget.Kind.CLASS) {
                String className = ai.target().asClass().name().toString();
                registerClass(className, reflectiveClasses);
                driverNames.add(className);
            }
        }

        if (!driverNames.isEmpty()) {
            log.infof("Morphium: passing %d @Driver classes for native-image ClassGraphCache pre-population", driverNames.size());
        }
        recorder.setDriverClassNames(driverNames);
    }

    // ------------------------------------------------------------------
    // GraalVM native image: @Messaging class discovery and pre-population
    // ------------------------------------------------------------------

    /**
     * Discovers all {@code @Messaging}-annotated classes at build time via Jandex,
     * registers them for GraalVM reflection, and passes the list to the recorder
     * so that {@link MorphiumProducer} can pre-populate {@code ClassGraphCache}
     * before {@code Morphium} is constructed. This bypasses the ClassGraph
     * classpath scan that fails in native mode.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void registerMessagingForNative(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                                    CombinedIndexBuildItem combinedIndex,
                                    MorphiumRecorder recorder) {
        IndexView index = combinedIndex.getIndex();
        DotName messagingDotName = DotName.createSimple(Messaging.class.getName());
        List<String> messagingNames = new ArrayList<>();

        for (AnnotationInstance ai : index.getAnnotations(messagingDotName)) {
            if (ai.target().kind() == AnnotationTarget.Kind.CLASS) {
                String className = ai.target().asClass().name().toString();
                registerClass(className, reflectiveClasses);
                messagingNames.add(className);
            }
        }

        if (!messagingNames.isEmpty()) {
            log.infof("Morphium: passing %d @Messaging classes for native-image ClassGraphCache pre-population", messagingNames.size());
        }
        recorder.setMessagingClassNames(messagingNames);
    }

    // ------------------------------------------------------------------
    // GraalVM native image: @Capped class discovery and pre-population
    // ------------------------------------------------------------------

    /**
     * Discovers all {@code @Capped}-annotated classes at build time via Jandex
     * and passes the list (possibly empty) to the recorder so that
     * {@link MorphiumProducer} can pre-populate {@code ClassGraphCache}.
     *
     * <p>Even when no {@code @Capped} classes exist in the application,
     * pre-registering an empty list prevents {@code checkCapped()} from
     * triggering a live ClassGraph scan at startup, which crashes in native mode.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void registerCappedForNative(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                                 CombinedIndexBuildItem combinedIndex,
                                 MorphiumRecorder recorder) {
        IndexView index = combinedIndex.getIndex();
        DotName cappedDotName = DotName.createSimple(Capped.class.getName());
        List<String> cappedNames = new ArrayList<>();

        for (AnnotationInstance ai : index.getAnnotations(cappedDotName)) {
            if (ai.target().kind() == AnnotationTarget.Kind.CLASS) {
                String className = ai.target().asClass().name().toString();
                registerClass(className, reflectiveClasses);
                cappedNames.add(className);
            }
        }

        if (!cappedNames.isEmpty()) {
            log.infof("Morphium: passing %d @Capped classes for native-image ClassGraphCache pre-population", cappedNames.size());
        }
        // Always call setCappedClassNames (even with empty list) — pre-registering an empty
        // list prevents checkCapped() from falling through to a live ClassGraph scan.
        recorder.setCappedClassNames(cappedNames);
    }


    // ------------------------------------------------------------------
    // GraalVM native image: MongoCommand hierarchy reflection registration
    // ------------------------------------------------------------------

    /**
     * Registers {@code MongoCommand} and all its subclasses for GraalVM reflection.
     *
     * <p>{@code MongoCommand.asMap()} uses
     * {@code AnnotationAndReflectionHelper.getAllFields()} which calls
     * {@code Class.getDeclaredFields()} on every class in the hierarchy. In a native
     * image, {@code getDeclaredFields()} only returns fields registered for reflection.
     * Without this, the {@code $db} field (declared in {@code MongoCommand}) is silently
     * missing from the command document, causing MongoDB to reject every OP_MSG with
     * <em>"Error: 40571 — OP_MSG requests require a $db argument"</em>.
     *
     * <p>Uses Jandex {@code getAllKnownSubclasses()} on the indexed morphium-core JAR
     * to discover all concrete and abstract command classes automatically.
     */
    @BuildStep
    void registerMongoCommandsForReflection(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                                            CombinedIndexBuildItem combinedIndex) {
        IndexView index = combinedIndex.getIndex();
        DotName mongoCommandDotName = DotName.createSimple("de.caluga.morphium.driver.commands.MongoCommand");

        // Register MongoCommand itself (declares $db, coll, comment, $readPreference)
        registerClass(mongoCommandDotName.toString(), reflectiveClasses);

        // Register all subclasses (WriteMongoCommand, ReadMongoCommand, AdminMongoCommand,
        // and all concrete commands like FindCommand, InsertMongoCommand, etc.)
        int count = 1; // counting MongoCommand itself
        for (ClassInfo ci : index.getAllKnownSubclasses(mongoCommandDotName)) {
            registerClass(ci.name().toString(), reflectiveClasses);
            count++;
        }
        log.infof("Morphium: registered %d MongoCommand classes for reflection (native image)", count);
    }

    // ------------------------------------------------------------------
    // GraalVM native image: runtime initialization for Morphium internals
    // ------------------------------------------------------------------

    /**
     * Registers Morphium-internal classes that must be initialized at run time
     * in GraalVM native images.
     *
     * <p>These classes have static fields (e.g. {@code AnnotationAndReflectionHelper},
     * {@code ScanResult}) that cannot be captured in the image heap because they
     * either hold ClassGraph scan results, ZipFile handles, or other runtime-only state.
     *
     * <p>By registering them here, users of quarkus-morphium do not need to add
     * {@code --initialize-at-run-time} entries to their {@code application.properties}.
     */
    @BuildStep
    void registerRuntimeInitializedClasses(
            BuildProducer<RuntimeInitializedClassBuildItem> runtimeInitClasses,
            BuildProducer<RuntimeInitializedPackageBuildItem> runtimeInitPackages) {

        // Morphium core classes with static AnnotationAndReflectionHelper or ClassGraph state
        String[] morphiumClasses = {
            "de.caluga.morphium.ObjectMapperImpl",
            "de.caluga.morphium.AnnotationAndReflectionHelper",
            "de.caluga.morphium.ClassGraphCache",
            "de.caluga.morphium.driver.commands.MongoCommand",
            "de.caluga.morphium.driver.wire.HelloResult",
            "de.caluga.morphium.IndexDescription"
        };

        for (String className : morphiumClasses) {
            runtimeInitClasses.produce(new RuntimeInitializedClassBuildItem(className));
        }

        // ClassGraph: static fields hold ZipFile/ScanResult objects that cannot be
        // serialized into the native image heap
        runtimeInitPackages.produce(new RuntimeInitializedPackageBuildItem("io.github.classgraph"));
    }

    /**
     * Registers all superclasses of a Morphium entity for GraalVM reflection.
     *
     * <p>Morphium's {@code AnnotationAndReflectionHelper.getAllFields()} walks the entire class
     * hierarchy via {@code getDeclaredFields()} on each level. If a superclass declares the
     * {@code @Id} field (common pattern: {@code BaseEntity} with {@code @Id String id}), that
     * field is invisible in a native image unless the superclass is also registered.
     *
     * <p>Stops at {@code java.lang.Object} and skips JDK/library classes.
     */
    private void registerSuperclasses(ClassInfo classInfo, IndexView index,
                                       BuildProducer<ReflectiveClassBuildItem> out,
                                       Set<String> alreadyRegistered) {
        DotName superName = classInfo.superName();
        while (superName != null && !superName.toString().equals("java.lang.Object")) {
            String superClassName = superName.toString();
            if (!alreadyRegistered.add(superClassName)) {
                break; // already processed this branch
            }
            log.debugf("Morphium: registering entity superclass %s for reflection", superClassName);
            registerClass(superClassName, out);

            // Walk further up the hierarchy via Jandex (if indexed) or stop
            ClassInfo superInfo = index.getClassByName(superName);
            if (superInfo == null) {
                break; // not in the Jandex index — JDK or non-indexed library class
            }
            superName = superInfo.superName();
        }
    }

    /**
     * Registers every subclass (direct and transitive) of a class annotated {@code @Entity} or
     * {@code @Embedded}, even though those subclasses carry no annotation of their own.
     *
     * <p>{@code @Entity}/{@code @Embedded} are not {@code @Inherited} (Java annotation
     * inheritance does not apply to types), but Morphium's own
     * {@code AnnotationAndReflectionHelper.isAnnotationPresentInHierarchy()} walks the class
     * hierarchy manually and treats a subclass as an entity/embedded type purely because a
     * superclass carries the annotation -- Morphium fully supports polymorphic persistence this
     * way. The Jandex scan above only ever finds classes that carry the annotation directly, so
     * without this, storing/loading an actual runtime instance of an unannotated subclass would
     * reflectively access fields/constructors never registered for native image, and crash only
     * in a native build, only for that specific subclass, only once such an instance is
     * actually persisted.
     */
    void registerSubclasses(ClassInfo classInfo, IndexView index,
                                     BuildProducer<ReflectiveClassBuildItem> out,
                                     Set<String> alreadyRegistered) {
        for (ClassInfo subclass : index.getAllKnownSubclasses(classInfo.name())) {
            String subclassName = subclass.name().toString();
            if (!alreadyRegistered.add(subclassName)) {
                continue; // already processed (e.g. reached via a different entity's hierarchy)
            }
            log.debugf("Morphium: registering entity subclass %s for reflection", subclassName);
            registerClass(subclassName, out);
        }
    }

    /**
     * Registers a custom {@code @Entity(nameProvider = ...)} class for reflection.
     *
     * <p>{@code ObjectMapperImpl.getNameProviderForClass()} instantiates the configured
     * {@code nameProvider} via {@code getDeclaredConstructor().newInstance()}, same as
     * {@code DefaultNameProvider} (already unconditionally registered above) -- but a custom
     * provider a user points {@code @Entity(nameProvider = ...)} at was never registered at
     * all, so a native-image build would fail at runtime the first time that entity's
     * collection name is resolved. {@code DefaultNameProvider} itself is skipped here since it
     * is already registered unconditionally.
     */
    void registerCustomNameProvider(AnnotationInstance entityAnnotation,
                                            BuildProducer<ReflectiveClassBuildItem> out) {
        AnnotationValue nameProviderValue = entityAnnotation.value("nameProvider");
        if (nameProviderValue == null) {
            return;
        }
        String nameProviderClassName = nameProviderValue.asClass().name().toString();
        if (nameProviderClassName.equals(DefaultNameProvider.class.getName())) {
            return;
        }
        registerClass(nameProviderClassName, out);
    }

    private void registerClass(String className,
                               BuildProducer<ReflectiveClassBuildItem> out) {
        log.debugf("Morphium: registering %s for reflection (native image)", className);
        out.produce(ReflectiveClassBuildItem.builder(className)
            .constructors(true)
            .methods(true)
            .fields(true)
            .build());
    }
}
