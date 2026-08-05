package de.caluga.morphium.quarkus.deployment;

import de.caluga.morphium.data.AbstractMorphiumRepository;
import de.caluga.morphium.data.FindMethodBridge;
import de.caluga.morphium.data.JdqlMethodBridge;
import de.caluga.morphium.data.MethodNameParser;
import de.caluga.morphium.data.QueryDescriptor;
import de.caluga.morphium.data.QueryMethodBridge;
import de.caluga.morphium.data.RepositoryMetadata;
import de.caluga.morphium.quarkus.data.QuarkusMorphiumRepository;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.GeneratedClassGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldCreator;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.FieldInfo;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.MethodParameterInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.PrimitiveType;
import org.jboss.jandex.Type;
import org.jboss.jandex.TypeVariable;
import org.jboss.logging.Logger;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

/**
 * Build-time processor for Jakarta Data {@code @Repository} interfaces.
 * <p>
 * Discovers repository interfaces via Jandex, validates them, and generates
 * implementation classes via Gizmo that extend {@link AbstractMorphiumRepository}
 * and delegate to its {@code doXxx()} methods.
 */
public class MorphiumDataProcessor {

    private static final Logger log = Logger.getLogger(MorphiumDataProcessor.class);

    private static final DotName REPOSITORY_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.Repository");
    private static final DotName DATA_REPOSITORY = DotName.createSimple(
            "jakarta.data.repository.DataRepository");
    private static final DotName BASIC_REPOSITORY = DotName.createSimple(
            "jakarta.data.repository.BasicRepository");
    private static final DotName CRUD_REPOSITORY = DotName.createSimple(
            "jakarta.data.repository.CrudRepository");
    private static final DotName MORPHIUM_REPOSITORY = DotName.createSimple(
            "de.caluga.morphium.data.MorphiumRepository");
    private static final DotName ENTITY_ANNOTATION = DotName.createSimple(
            "de.caluga.morphium.annotations.Entity");
    private static final DotName ID_ANNOTATION = DotName.createSimple(
            "de.caluga.morphium.annotations.Id");

    // Jakarta Data lifecycle/query annotations
    private static final DotName FIND_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.Find");
    private static final DotName BY_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.By");
    private static final DotName ORDER_BY_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.OrderBy");
    private static final DotName ORDER_BY_LIST_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.OrderBy$List");
    private static final DotName DELETE_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.Delete");
    private static final DotName INSERT_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.Insert");
    private static final DotName SAVE_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.Save");
    private static final DotName UPDATE_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.Update");
    private static final DotName QUERY_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.Query");
    private static final DotName PARAM_ANNOTATION = DotName.createSimple(
            "jakarta.data.repository.Param");

    // Special parameter types
    private static final DotName SORT_TYPE = DotName.createSimple("jakarta.data.Sort");
    private static final DotName ORDER_TYPE = DotName.createSimple("jakarta.data.Order");
    private static final DotName PAGE_REQUEST_TYPE = DotName.createSimple("jakarta.data.page.PageRequest");
    private static final DotName LIMIT_TYPE = DotName.createSimple("jakarta.data.Limit");
    private static final DotName PAGE_TYPE = DotName.createSimple("jakarta.data.page.Page");
    private static final DotName CURSORED_PAGE_TYPE = DotName.createSimple("jakarta.data.page.CursoredPage");
    private static final DotName COMPLETION_STAGE_TYPE = DotName.createSimple("java.util.concurrent.CompletionStage");

    // Metamodel types
    private static final String STATIC_METAMODEL_ANN = "jakarta.data.metamodel.StaticMetamodel";
    private static final String ATTRIBUTE_CLASS = "jakarta.data.metamodel.Attribute";
    private static final String SORTABLE_ATTRIBUTE_CLASS = "jakarta.data.metamodel.SortableAttribute";
    private static final String TEXT_ATTRIBUTE_CLASS = "jakarta.data.metamodel.TextAttribute";
    private static final String ATTRIBUTE_RECORD_CLASS = "jakarta.data.metamodel.impl.AttributeRecord";
    private static final String SORTABLE_ATTRIBUTE_RECORD_CLASS = "jakarta.data.metamodel.impl.SortableAttributeRecord";
    private static final String TEXT_ATTRIBUTE_RECORD_CLASS = "jakarta.data.metamodel.impl.TextAttributeRecord";

    // Morphium @Transient and @Property annotations
    private static final DotName TRANSIENT_ANNOTATION = DotName.createSimple(
            "de.caluga.morphium.annotations.Transient");
    private static final DotName PROPERTY_ANNOTATION = DotName.createSimple(
            "de.caluga.morphium.annotations.Property");

    // Common types for metamodel classification
    private static final Set<String> SORTABLE_TYPES = Set.of(
            "byte", "short", "int", "long", "float", "double", "char", "boolean",
            "java.lang.Byte", "java.lang.Short", "java.lang.Integer", "java.lang.Long",
            "java.lang.Float", "java.lang.Double", "java.lang.Character", "java.lang.Boolean",
            "java.math.BigDecimal", "java.math.BigInteger",
            "java.time.LocalDate", "java.time.LocalDateTime", "java.time.LocalTime",
            "java.time.Instant", "java.time.ZonedDateTime", "java.time.OffsetDateTime",
            "java.util.Date");

    // Standard CRUD/Basic method names that are handled by delegation
    private static final Set<String> CRUD_METHODS = Set.of(
            "findById", "findAll", "save", "saveAll", "delete", "deleteById", "deleteAll",
            "insert", "insertAll", "update", "updateAll",
            "distinct", "morphium", "query");

    // -----------------------------------------------------------------
    // Step 1: Discover @Repository interfaces
    // -----------------------------------------------------------------

    @BuildStep
    void discoverRepositories(CombinedIndexBuildItem combinedIndex,
                              BuildProducer<RepositoryBuildItem> repositoryProducer) {
        IndexView index = combinedIndex.getIndex();

        for (AnnotationInstance ann : index.getAnnotations(REPOSITORY_ANNOTATION)) {
            if (ann.target().kind() != AnnotationTarget.Kind.CLASS) continue;

            ClassInfo repoClass = ann.target().asClass();
            if (!repoClass.isInterface()) {
                log.warnf("@Repository on non-interface %s — skipping", repoClass.name());
                continue;
            }

            // Find the DataRepository/BasicRepository/CrudRepository superinterface and extract T, K
            TypeParameters tp = resolveEntityAndIdTypes(repoClass, index);
            if (tp == null) {
                log.warnf("@Repository %s does not extend DataRepository/BasicRepository/CrudRepository — skipping",
                        repoClass.name());
                continue;
            }

            // Find @Id field on entity class
            ClassInfo entityClassInfo = index.getClassByName(tp.entityType);
            if (entityClassInfo == null) {
                throw new IllegalStateException(
                        "@Repository " + repoClass.name() + " references entity " + tp.entityType
                        + " which is not in the Jandex index. Ensure it is annotated with @Entity.");
            }

            String idFieldName = findIdField(entityClassInfo, index);
            if (idFieldName == null) {
                throw new IllegalStateException(
                        "Entity " + tp.entityType + " referenced by @Repository " + repoClass.name()
                        + " has no @Id field.");
            }

            log.infof("Discovered @Repository %s → entity=%s, id=%s, idField=%s",
                    repoClass.name(), tp.entityType, tp.idType, idFieldName);

            repositoryProducer.produce(new RepositoryBuildItem(
                    repoClass.name().toString(),
                    tp.entityType.toString(),
                    tp.idType.toString(),
                    idFieldName));
        }
    }

    // -----------------------------------------------------------------
    // Step 2: Generate repository implementations via Gizmo
    // -----------------------------------------------------------------

    @BuildStep
    void generateRepositoryImpls(List<RepositoryBuildItem> repositories,
                                 CombinedIndexBuildItem combinedIndex,
                                 BuildProducer<GeneratedBeanBuildItem> generatedBeans,
                                 BuildProducer<ReflectiveClassBuildItem> reflectiveClasses,
                                 BuildProducer<AdditionalBeanBuildItem> additionalBeans) {
        if (repositories.isEmpty()) return;

        // Register QuarkusMorphiumRepository as a bean
        additionalBeans.produce(AdditionalBeanBuildItem.builder()
                .addBeanClass(QuarkusMorphiumRepository.class)
                .setUnremovable()
                .build());

        IndexView index = combinedIndex.getIndex();
        ClassOutput classOutput = new GeneratedBeanGizmoAdaptor(generatedBeans);

        for (RepositoryBuildItem repo : repositories) {
            generateImpl(repo, index, classOutput, reflectiveClasses);
        }
    }

    // -----------------------------------------------------------------
    // Step 3: Generate @StaticMetamodel classes
    // -----------------------------------------------------------------

    @BuildStep
    void generateStaticMetamodels(List<RepositoryBuildItem> repositories,
                                   CombinedIndexBuildItem combinedIndex,
                                   BuildProducer<GeneratedClassBuildItem> generatedClasses,
                                   BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        if (repositories.isEmpty()) return;

        IndexView index = combinedIndex.getIndex();
        ClassOutput classOutput = new GeneratedClassGizmoAdaptor(generatedClasses, true);

        // Collect unique entity classes
        Set<String> processedEntities = new LinkedHashSet<>();
        for (RepositoryBuildItem repo : repositories) {
            String entityClassName = repo.getEntityClassName();
            if (processedEntities.add(entityClassName)) {
                ClassInfo entityClass = index.getClassByName(DotName.createSimple(entityClassName));
                if (entityClass != null) {
                    generateMetamodel(entityClassName, entityClass, index, classOutput, reflectiveClasses);
                }
            }
        }
    }

    private void generateMetamodel(String entityClassName,
                                    ClassInfo entityClass,
                                    IndexView index,
                                    ClassOutput classOutput,
                                    BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        String metamodelClassName = entityClassName + "_";

        try (ClassCreator cc = ClassCreator.builder()
                .classOutput(classOutput)
                .className(metamodelClassName)
                .superClass(Object.class)
                .build()) {

            // Add @StaticMetamodel(EntityClass.class) annotation
            cc.addAnnotation(STATIC_METAMODEL_ANN)
                    .addValue("value", AnnotationValue.createClassValue("value",
                            Type.create(DotName.createSimple(entityClassName), Type.Kind.CLASS)));

            // Collect persistent fields from entity hierarchy
            List<MetamodelField> fields = collectMetamodelFields(entityClass, index);

            // Generate String constants (public static final String FIELD_NAME = "javaName")
            for (MetamodelField mf : fields) {
                FieldCreator fc = cc.getFieldCreator(mf.constantName, String.class);
                fc.setModifiers(Modifier.PUBLIC
                        | Modifier.STATIC
                        | Modifier.FINAL);
            }

            // Generate Attribute fields (public static final XxxAttribute<Entity> field)
            for (MetamodelField mf : fields) {
                String attributeType = mf.attributeInterfaceType();
                FieldCreator fc = cc.getFieldCreator(mf.javaName, attributeType);
                fc.setModifiers(Modifier.PUBLIC
                        | Modifier.STATIC
                        | Modifier.FINAL);
            }

            // Generate static initializer <clinit>
            try (MethodCreator clinit = cc.getMethodCreator("<clinit>", void.class)) {
                clinit.setModifiers(Modifier.STATIC);

                for (MetamodelField mf : fields) {
                    // Assign String constant: FIELD_NAME = "javaName"
                    ResultHandle nameValue = clinit.load(mf.javaName);
                    clinit.writeStaticField(
                            FieldDescriptor.of(metamodelClassName, mf.constantName, String.class),
                            nameValue);

                    // Create attribute record: new XxxAttributeRecord<>("javaName")
                    String recordClass = mf.attributeRecordType();
                    ResultHandle attrInstance = clinit.newInstance(
                            MethodDescriptor.ofConstructor(recordClass, String.class),
                            nameValue);

                    // Assign: field = new XxxAttributeRecord<>("javaName")
                    clinit.writeStaticField(
                            FieldDescriptor.of(metamodelClassName, mf.javaName, mf.attributeInterfaceType()),
                            attrInstance);
                }

                clinit.returnVoid();
            }

            log.infof("Generated @StaticMetamodel: %s", metamodelClassName);
        }

        reflectiveClasses.produce(ReflectiveClassBuildItem.builder(metamodelClassName)
                .constructors(true).methods(true).fields(true).build());
    }

    private record MetamodelField(String javaName, String constantName, FieldCategory category) {

        String attributeInterfaceType() {
            return switch (category) {
                case TEXT -> TEXT_ATTRIBUTE_CLASS;
                case SORTABLE -> SORTABLE_ATTRIBUTE_CLASS;
                case BASIC -> ATTRIBUTE_CLASS;
            };
        }

        String attributeRecordType() {
            return switch (category) {
                case TEXT -> TEXT_ATTRIBUTE_RECORD_CLASS;
                case SORTABLE -> SORTABLE_ATTRIBUTE_RECORD_CLASS;
                case BASIC -> ATTRIBUTE_RECORD_CLASS;
            };
        }
    }

    private enum FieldCategory { TEXT, SORTABLE, BASIC }

    private List<MetamodelField> collectMetamodelFields(ClassInfo entityClass, IndexView index) {
        List<MetamodelField> result = new ArrayList<>();
        ClassInfo current = entityClass;

        while (current != null) {
            for (FieldInfo field : current.fields()) {
                // Skip static, transient, and @Transient fields
                if (Modifier.isStatic(field.flags())) continue;
                if (Modifier.isTransient(field.flags())) continue;
                if (field.hasAnnotation(TRANSIENT_ANNOTATION)) continue;

                String javaName = field.name();
                String constantName = toUpperSnakeCase(javaName);
                FieldCategory category = classifyField(field, index);

                result.add(new MetamodelField(javaName, constantName, category));
            }
            DotName superName = current.superName();
            if (superName == null || superName.toString().equals("java.lang.Object")) break;
            current = index.getClassByName(superName);
        }

        return result;
    }

    private FieldCategory classifyField(FieldInfo field, IndexView index) {
        String typeName = field.type().name().toString();

        if ("java.lang.String".equals(typeName) || "char".equals(typeName)
                || "java.lang.Character".equals(typeName)) {
            return FieldCategory.TEXT;
        }

        if (SORTABLE_TYPES.contains(typeName)) {
            return FieldCategory.SORTABLE;
        }

        // Check if it's an enum (enums are sortable)
        ClassInfo typeInfo = index.getClassByName(field.type().name());
        if (typeInfo != null && typeInfo.isEnum()) {
            return FieldCategory.SORTABLE;
        }

        return FieldCategory.BASIC;
    }

    private static String toUpperSnakeCase(String camelCase) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                sb.append('_');
            }
            sb.append(Character.toUpperCase(c));
        }
        return sb.toString();
    }

    // -----------------------------------------------------------------
    // Gizmo code generation
    // -----------------------------------------------------------------

    private void generateImpl(RepositoryBuildItem repo,
                              IndexView index,
                              ClassOutput classOutput,
                              BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {

        String implClassName = repo.getInterfaceName() + "_MorphiumImpl";
        String entityClassName = repo.getEntityClassName();
        String idClassName = repo.getIdClassName();
        String idFieldName = repo.getIdFieldName();

        // Determine which level of the repository hierarchy this implements
        ClassInfo repoInterface = index.getClassByName(DotName.createSimple(repo.getInterfaceName()));
        boolean isMorphium = implementsInterface(repoInterface, MORPHIUM_REPOSITORY, index);
        boolean isCrud = isMorphium || implementsInterface(repoInterface, CRUD_REPOSITORY, index);
        boolean isBasic = isCrud || implementsInterface(repoInterface, BASIC_REPOSITORY, index);

        String superClass = QuarkusMorphiumRepository.class.getName();
        String signature = buildGenericSignature(superClass, repo.getInterfaceName(),
                entityClassName, idClassName);

        try (ClassCreator cc = ClassCreator.builder()
                .classOutput(classOutput)
                .className(implClassName)
                .superClass(superClass)
                .interfaces(repo.getInterfaceName())
                .signature(signature)
                .build()) {

            cc.addAnnotation("jakarta.enterprise.context.ApplicationScoped");

            // Constructor: super(new RepositoryMetadata(Entity.class, Id.class, "idField"))
            generateConstructor(cc, entityClassName, idClassName, idFieldName);

            // BasicRepository methods
            if (isBasic) {
                generateFindById(cc);
                generateFindAll(cc);
                generateFindAllPaged(cc);
                // Check if repo declares findAll returning CursoredPage
                if (repoInterface != null && hasFindAllCursored(repoInterface)) {
                    generateFindAllCursored(cc);
                }
                generateSave(cc);
                generateSaveAll(cc);
                generateDelete(cc);
                generateDeleteById(cc);
                generateDeleteAll(cc);
                generateDeleteAllNoArg(cc);
            }

            // CrudRepository methods
            if (isCrud) {
                generateInsert(cc);
                generateInsertAll(cc);
                generateUpdate(cc);
                generateUpdateAll(cc);
            }

            // MorphiumRepository methods
            if (isMorphium) {
                generateDistinct(cc);
                generateMorphium(cc);
                generateQuery(cc);
            }

            // Custom query methods
            if (repoInterface != null) {
                Set<String> entityFields = collectEntityFields(
                        index.getClassByName(DotName.createSimple(entityClassName)), index);
                generateCustomQueryMethods(cc, repoInterface, index, entityClassName, entityFields, reflectiveClasses);
            }

            log.infof("Generated repository implementation: %s", implClassName);
        }

        // Register for reflection (native image)
        reflectiveClasses.produce(ReflectiveClassBuildItem.builder(implClassName)
                .constructors(true).methods(true).fields(true).build());
    }

    // -- Constructor generation --

    private void generateConstructor(ClassCreator cc,
                                     String entityClassName,
                                     String idClassName,
                                     String idFieldName) {
        try (MethodCreator ctor = cc.getMethodCreator("<init>", void.class)) {
            ctor.setModifiers(Modifier.PUBLIC);

            ResultHandle entityClass = ctor.loadClassFromTCCL(entityClassName);
            ResultHandle idClass = ctor.loadClassFromTCCL(idClassName);
            ResultHandle idField = ctor.load(idFieldName);

            ResultHandle metadata = ctor.newInstance(
                    MethodDescriptor.ofConstructor(RepositoryMetadata.class,
                            Class.class, Class.class, String.class),
                    entityClass, idClass, idField);

            ctor.invokeSpecialMethod(
                    MethodDescriptor.ofMethod(QuarkusMorphiumRepository.class,
                            "<init>", void.class, RepositoryMetadata.class),
                    ctor.getThis(), metadata);

            ctor.returnVoid();
        }
    }

    // -- BasicRepository method generation --
    // Jakarta Data 1.0: findById returns Optional<T>, save returns <S extends T> S, etc.

    private void generateFindById(ClassCreator cc) {
        // Optional<T> findById(K id)
        try (MethodCreator mc = cc.getMethodCreator("findById", Optional.class, Object.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doFindById", Optional.class, Object.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnValue(result);
        }
    }

    private void generateFindAll(ClassCreator cc) {
        // Stream<T> findAll()
        try (MethodCreator mc = cc.getMethodCreator("findAll", Stream.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doFindAll", Stream.class),
                    mc.getThis());
            mc.returnValue(result);
        }
    }

    private void generateFindAllPaged(ClassCreator cc) {
        // Page<T> findAll(PageRequest pageRequest, Order<T> sortBy)
        String pageClass = "jakarta.data.page.Page";
        String pageRequestClass = "jakarta.data.page.PageRequest";
        String orderClass = "jakarta.data.Order";
        try (MethodCreator mc = cc.getMethodCreator("findAll", pageClass,
                pageRequestClass, orderClass)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doFindAllPaged", "jakarta.data.page.Page",
                            "jakarta.data.page.PageRequest", "jakarta.data.Order"),
                    mc.getThis(), mc.getMethodParam(0), mc.getMethodParam(1));
            mc.returnValue(result);
        }
    }

    private boolean hasFindAllCursored(ClassInfo repoInterface) {
        for (MethodInfo method : repoInterface.methods()) {
            if ("findAll".equals(method.name())
                    && method.returnType().name().equals(CURSORED_PAGE_TYPE)) {
                return true;
            }
        }
        return false;
    }

    private void generateFindAllCursored(ClassCreator cc) {
        // CursoredPage<T> findAll(PageRequest pageRequest, Order<T> sortBy)
        String cursoredPageClass = "jakarta.data.page.CursoredPage";
        String pageRequestClass = "jakarta.data.page.PageRequest";
        String orderClass = "jakarta.data.Order";
        try (MethodCreator mc = cc.getMethodCreator("findAll", cursoredPageClass,
                pageRequestClass, orderClass)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doFindAllCursored", "jakarta.data.page.CursoredPage",
                            "jakarta.data.page.PageRequest", "jakarta.data.Order"),
                    mc.getThis(), mc.getMethodParam(0), mc.getMethodParam(1));
            mc.returnValue(result);
        }
    }

    private void warnIfMissingIdInOrderBy(MethodInfo method, String orderBySpec,
                                           String entityClassName, Set<String> entityFields) {
        // Check if "id" field is included in the orderBy spec
        Set<String> orderByFields = new HashSet<>();
        for (String part : orderBySpec.split(",")) {
            String[] fieldAndDir = part.split(":");
            orderByFields.add(fieldAndDir[0]);
        }
        if (!orderByFields.contains("id")) {
            log.warnf("CursoredPage method %s.%s has @OrderBy %s but does not include the @Id field 'id'. "
                            + "Without a unique tie-breaker, cursor-based pagination may produce duplicate or missing results. "
                            + "Consider adding @OrderBy(\"id\") as last sort criterion.",
                    method.declaringClass().name(), method.name(), orderByFields);
        }
    }

    private void generateSave(ClassCreator cc) {
        // <S extends T> S save(S entity)
        try (MethodCreator mc = cc.getMethodCreator("save", Object.class, Object.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doSave", Object.class, Object.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnValue(result);
        }
    }

    private void generateSaveAll(ClassCreator cc) {
        // <S extends T> List<S> saveAll(List<S> entities)
        try (MethodCreator mc = cc.getMethodCreator("saveAll", List.class, List.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doSaveAll", List.class, List.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnValue(result);
        }
    }

    private void generateDelete(ClassCreator cc) {
        // void delete(T entity)
        try (MethodCreator mc = cc.getMethodCreator("delete", void.class, Object.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doDelete", void.class, Object.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnVoid();
        }
    }

    private void generateDeleteById(ClassCreator cc) {
        // void deleteById(K id)
        try (MethodCreator mc = cc.getMethodCreator("deleteById", void.class, Object.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doDeleteById", void.class, Object.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnVoid();
        }
    }

    private void generateDeleteAll(ClassCreator cc) {
        // void deleteAll(List<? extends T> entities)
        try (MethodCreator mc = cc.getMethodCreator("deleteAll", void.class, List.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doDeleteAll", void.class, List.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnVoid();
        }
    }

    private void generateDeleteAllNoArg(ClassCreator cc) {
        // void deleteAll() — no-arg, clears entire collection
        try (MethodCreator mc = cc.getMethodCreator("deleteAll", void.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doDeleteAllNoArg", void.class),
                    mc.getThis());
            mc.returnVoid();
        }
    }

    // -- CrudRepository method generation --

    private void generateInsert(ClassCreator cc) {
        try (MethodCreator mc = cc.getMethodCreator("insert", Object.class, Object.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doInsert", Object.class, Object.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnValue(result);
        }
    }

    private void generateInsertAll(ClassCreator cc) {
        try (MethodCreator mc = cc.getMethodCreator("insertAll", List.class, List.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doInsertAll", List.class, List.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnValue(result);
        }
    }

    private void generateUpdate(ClassCreator cc) {
        try (MethodCreator mc = cc.getMethodCreator("update", Object.class, Object.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doUpdate", Object.class, Object.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnValue(result);
        }
    }

    private void generateUpdateAll(ClassCreator cc) {
        try (MethodCreator mc = cc.getMethodCreator("updateAll", List.class, List.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doUpdateAll", List.class, List.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnValue(result);
        }
    }

    // -- MorphiumRepository method generation --

    private void generateDistinct(ClassCreator cc) {
        // List<Object> distinct(String fieldName)
        try (MethodCreator mc = cc.getMethodCreator("distinct", List.class, String.class)) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doDistinct", List.class, String.class),
                    mc.getThis(), mc.getMethodParam(0));
            mc.returnValue(result);
        }
    }

    private void generateMorphium(ClassCreator cc) {
        // Morphium morphium()
        try (MethodCreator mc = cc.getMethodCreator("morphium",
                "de.caluga.morphium.Morphium")) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doMorphium", "de.caluga.morphium.Morphium"),
                    mc.getThis());
            mc.returnValue(result);
        }
    }

    private void generateQuery(ClassCreator cc) {
        // Query<T> query()
        try (MethodCreator mc = cc.getMethodCreator("query",
                "de.caluga.morphium.query.Query")) {
            mc.setModifiers(Modifier.PUBLIC);
            ResultHandle result = mc.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                            "doQuery", "de.caluga.morphium.query.Query"),
                    mc.getThis());
            mc.returnValue(result);
        }
    }

    // -----------------------------------------------------------------
    // Custom query method generation
    // -----------------------------------------------------------------

    private void generateCustomQueryMethods(ClassCreator cc,
                                            ClassInfo repoInterface,
                                            IndexView index,
                                            String entityClassName,
                                            Set<String> entityFields,
                                            BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        for (MethodInfo method : repoInterface.methods()) {
            String name = method.name();

            // Skip standard CRUD methods and default/static methods
            if (CRUD_METHODS.contains(name)) continue;
            if (method.isDefault()) continue;
            if (Modifier.isStatic(method.flags())) continue;

            // Phase 5: @Query with JDQL
            if (method.hasAnnotation(QUERY_ANNOTATION)) {
                generateQueryAnnotatedMethod(cc, method, entityClassName, index, reflectiveClasses);
                continue;
            }

            // Phase 4: Check for annotation-based methods first
            if (method.hasAnnotation(FIND_ANNOTATION)) {
                generateFindAnnotatedMethod(cc, method, entityClassName, entityFields);
                continue;
            }
            if (method.hasAnnotation(DELETE_ANNOTATION)) {
                generateDeleteAnnotatedMethod(cc, method, entityClassName);
                continue;
            }
            if (method.hasAnnotation(INSERT_ANNOTATION)) {
                generateInsertAnnotatedMethod(cc, method);
                continue;
            }
            if (method.hasAnnotation(SAVE_ANNOTATION)) {
                generateSaveAnnotatedMethod(cc, method);
                continue;
            }
            if (method.hasAnnotation(UPDATE_ANNOTATION)) {
                generateUpdateAnnotatedMethod(cc, method);
                continue;
            }

            // Phase 2: Try to parse as query derivation method
            if (name.startsWith("findBy") || name.startsWith("countBy")
                    || name.startsWith("existsBy") || name.startsWith("deleteBy")) {
                generateQueryMethod(cc, method, entityClassName, entityFields);
            }
        }
    }

    private void generateQueryMethod(ClassCreator cc,
                                     MethodInfo method,
                                     String entityClassName,
                                     Set<String> entityFields) {
        String methodName = method.name();

        // Build orderBy spec from @OrderBy annotations
        String orderBySpec = buildOrderBySpec(method);

        // Detect async: CompletionStage<X> → unwrap X as effective return type
        Type returnType = method.returnType();
        boolean isAsync = isCompletionStage(returnType);
        Type effectiveReturnType = isAsync ? unwrapCompletionStage(returnType) : returnType;

        // Strip "Async" suffix for parsing (e.g. "findByStatusAsync" → "findByStatus")
        String parseableName = isAsync && methodName.endsWith("Async")
                ? methodName.substring(0, methodName.length() - 5) : methodName;

        // Parse method name at build time to validate it
        QueryDescriptor descriptor;
        try {
            descriptor = MethodNameParser.parse(parseableName, entityFields);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    "Failed to parse repository method " + method.declaringClass().name()
                    + "." + methodName + ": " + e.getMessage(), e);
        }

        // Determine return type for the descriptor (based on effective/inner type)
        boolean returnsOptional = isOptional(effectiveReturnType);
        boolean returnsStream = isStream(effectiveReturnType);
        boolean returnsSingle = !isList(effectiveReturnType) && !returnsStream
                && !returnsOptional
                && descriptor.prefix() == QueryDescriptor.Prefix.FIND;

        // Build actual parameter type descriptors from the Jandex method info
        String[] paramTypeNames = new String[method.parametersCount()];
        for (int i = 0; i < method.parametersCount(); i++) {
            paramTypeNames[i] = toDescriptorName(method.parameterType(i));
        }
        String returnTypeName = toDescriptorName(returnType);

        try (MethodCreator mc = cc.getMethodCreator(
                MethodDescriptor.ofMethod(cc.getClassName(), methodName,
                        returnTypeName, paramTypeNames))) {
            mc.setModifiers(Modifier.PUBLIC);

            // Build args array: Object[] args = new Object[] { param0, param1, ... }
            ResultHandle argsArray = mc.newArray(Object.class, mc.load(method.parametersCount()));
            for (int i = 0; i < method.parametersCount(); i++) {
                ResultHandle param = mc.getMethodParam(i);
                // Box primitives if needed
                Type paramType = method.parameterType(i);
                if (paramType.kind() == Type.Kind.PRIMITIVE) {
                    param = boxPrimitive(mc, param, paramType.asPrimitiveType());
                }
                mc.writeArrayValue(argsArray, i, param);
            }

            // Determine if this is a deleteBy* returning boolean (needs count-to-boolean conversion)
            boolean returnsBoolean = effectiveReturnType.kind() == Type.Kind.PRIMITIVE
                    && effectiveReturnType.asPrimitiveType().primitive() == PrimitiveType.Primitive.BOOLEAN
                    && descriptor.prefix() == QueryDescriptor.Prefix.DELETE;

            ResultHandle methodNameHandle = mc.load(parseableName);
            ResultHandle returnsSingleHandle = mc.load(returnsSingle);
            ResultHandle returnsOptionalHandle = mc.load(returnsOptional);
            ResultHandle returnsBooleanHandle = mc.load(returnsBoolean);
            ResultHandle returnsStreamHandle = mc.load(returnsStream);
            ResultHandle orderBySpecHandle = mc.load(orderBySpec);
            ResultHandle thisHandle = mc.getThis();

            String bridgeMethod = isAsync ? "executeQueryAsync" : "executeQuery";
            Class<?> bridgeReturnType = isAsync ? CompletionStage.class : Object.class;

            // Handle void return type (e.g., void deleteByStatus(...))
            if (returnType.kind() == Type.Kind.VOID) {
                mc.invokeStaticMethod(
                        MethodDescriptor.ofMethod(
                                QueryMethodBridge.class,
                                "executeQuery",
                                Object.class,
                                AbstractMorphiumRepository.class,
                                String.class,
                                Object[].class,
                                boolean.class,
                                boolean.class,
                                boolean.class,
                                boolean.class,
                                String.class),
                        thisHandle, methodNameHandle, argsArray, returnsSingleHandle,
                        returnsOptionalHandle, returnsBooleanHandle, returnsStreamHandle,
                        orderBySpecHandle);
                mc.returnVoid();
            } else {
                ResultHandle result = mc.invokeStaticMethod(
                        MethodDescriptor.ofMethod(
                                QueryMethodBridge.class,
                                bridgeMethod,
                                bridgeReturnType,
                                AbstractMorphiumRepository.class,
                                String.class,
                                Object[].class,
                                boolean.class,
                                boolean.class,
                                boolean.class,
                                boolean.class,
                                String.class),
                        thisHandle, methodNameHandle, argsArray, returnsSingleHandle,
                        returnsOptionalHandle, returnsBooleanHandle, returnsStreamHandle,
                        orderBySpecHandle);

                // Unbox/cast the result to the declared return type (skip for async — returns CompletionStage)
                if (!isAsync && returnType.kind() == Type.Kind.PRIMITIVE) {
                    result = unboxPrimitive(mc, result, returnType.asPrimitiveType());
                }

                mc.returnValue(result);
            }
        }

        log.infof("Generated query-derivation method: %s.%s%s → %s (conditions: %d, orderBy: %s)",
                method.declaringClass().name(), methodName,
                isAsync ? " (async)" : "",
                descriptor.prefix().name().toLowerCase(Locale.ROOT),
                descriptor.conditions().size(),
                orderBySpec.isEmpty() ? "none" : orderBySpec);
    }

    private String toDescriptorName(Type type) {
        if (type.kind() == Type.Kind.PRIMITIVE) {
            return type.asPrimitiveType().primitive().name().toLowerCase(Locale.ROOT);
        }
        return type.name().toString();
    }

    private ResultHandle boxPrimitive(MethodCreator mc, ResultHandle value,
                                       PrimitiveType ptype) {
        return switch (ptype.primitive()) {
            case DOUBLE -> mc.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Double.class, "valueOf", Double.class, double.class), value);
            case FLOAT -> mc.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Float.class, "valueOf", Float.class, float.class), value);
            case LONG -> mc.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Long.class, "valueOf", Long.class, long.class), value);
            case INT -> mc.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Integer.class, "valueOf", Integer.class, int.class), value);
            case BOOLEAN -> mc.invokeStaticMethod(
                    MethodDescriptor.ofMethod(Boolean.class, "valueOf", Boolean.class, boolean.class), value);
            default -> value;
        };
    }

    private ResultHandle unboxPrimitive(MethodCreator mc, ResultHandle value,
                                         PrimitiveType ptype) {
        return switch (ptype.primitive()) {
            case LONG -> {
                ResultHandle cast = mc.checkCast(value, Long.class);
                yield mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(Long.class, "longValue", long.class), cast);
            }
            case DOUBLE -> {
                ResultHandle cast = mc.checkCast(value, Double.class);
                yield mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(Double.class, "doubleValue", double.class), cast);
            }
            case INT -> {
                ResultHandle cast = mc.checkCast(value, Integer.class);
                yield mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(Integer.class, "intValue", int.class), cast);
            }
            case BOOLEAN -> {
                ResultHandle cast = mc.checkCast(value, Boolean.class);
                yield mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(Boolean.class, "booleanValue", boolean.class), cast);
            }
            case FLOAT -> {
                ResultHandle cast = mc.checkCast(value, Float.class);
                yield mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(Float.class, "floatValue", float.class), cast);
            }
            default -> value;
        };
    }

    // -----------------------------------------------------------------
    // Phase 4: @Find, @Delete, @Insert, @Save, @Update generation
    // -----------------------------------------------------------------

    /**
     * Generates implementation for a {@code @Find} annotated method.
     * Parameters annotated with {@code @By} become equality conditions.
     * Special parameters (Sort, Order, PageRequest, Limit) are detected by type.
     */
    private void generateFindAnnotatedMethod(ClassCreator cc, MethodInfo method,
                                              String entityClassName,
                                              Set<String> entityFields) {
        // Build conditions spec and identify special params
        StringBuilder conditionsSpec = new StringBuilder();
        int conditionCount = 0;
        int sortParamIndex = -1;
        int orderParamIndex = -1;
        int pageRequestParamIndex = -1;
        int limitParamIndex = -1;

        for (int i = 0; i < method.parametersCount(); i++) {
            Type paramType = method.parameterType(i);
            DotName paramTypeName = paramType.name();

            // Check for special parameter types
            if (paramTypeName.equals(SORT_TYPE)) {
                sortParamIndex = i;
                continue;
            }
            if (paramTypeName.equals(ORDER_TYPE)) {
                orderParamIndex = i;
                continue;
            }
            if (paramTypeName.equals(PAGE_REQUEST_TYPE)) {
                pageRequestParamIndex = i;
                continue;
            }
            if (paramTypeName.equals(LIMIT_TYPE)) {
                limitParamIndex = i;
                continue;
            }

            // Check for @By annotation
            AnnotationInstance byAnn = method.parameters().get(i).annotation(BY_ANNOTATION);
            if (byAnn != null) {
                String fieldName = byAnn.value().asString();
                // Validate field exists — for dot-notation paths (e.g. "category.name")
                // only validate the root segment against entity fields
                if (entityFields != null && !entityFields.isEmpty() && !"id(this)".equals(fieldName)) {
                    String rootField = fieldName.contains(".") ? fieldName.substring(0, fieldName.indexOf('.')) : fieldName;
                    if (!entityFields.contains(rootField)) {
                        log.warnf("@By(\"%s\") on method %s.%s param %s — field '%s' not found on entity %s. " +
                                        "Will use as-is (may be resolved at runtime via @Property).",
                                fieldName, method.declaringClass().name(), method.name(), i, rootField, entityClassName);
                    }
                }
                if (conditionsSpec.length() > 0) conditionsSpec.append(",");
                conditionsSpec.append(fieldName).append(":").append(i);
                conditionCount++;
            }
        }

        // Build orderBy spec from @OrderBy annotations
        String orderBySpec = buildOrderBySpec(method);

        // Detect async: CompletionStage<X> → unwrap X as effective return type
        Type returnType = method.returnType();
        boolean isAsync = isCompletionStage(returnType);
        Type effectiveReturnType = isAsync ? unwrapCompletionStage(returnType) : returnType;

        // Determine return type (based on effective/inner type)
        boolean returnsOptional = isOptional(effectiveReturnType);
        boolean returnsCursoredPage = effectiveReturnType.name().equals(CURSORED_PAGE_TYPE);
        boolean returnsStream = isStream(effectiveReturnType);
        boolean returnsSingle = !isList(effectiveReturnType) && !returnsStream
                && !returnsOptional
                && !effectiveReturnType.name().equals(PAGE_TYPE)
                && !returnsCursoredPage;

        // Warn if CursoredPage method lacks @Id field in @OrderBy
        if (returnsCursoredPage && !orderBySpec.isEmpty()) {
            warnIfMissingIdInOrderBy(method, orderBySpec, entityClassName, entityFields);
        }

        // Build parameter type descriptors
        String[] paramTypeNames = new String[method.parametersCount()];
        for (int i = 0; i < method.parametersCount(); i++) {
            paramTypeNames[i] = toDescriptorName(method.parameterType(i));
        }
        String returnTypeName = toDescriptorName(returnType);

        String bridgeMethod = isAsync ? "executeFindAsync" : "executeFind";
        Class<?> bridgeReturnType = isAsync ? CompletionStage.class : Object.class;

        try (MethodCreator mc = cc.getMethodCreator(
                MethodDescriptor.ofMethod(cc.getClassName(), method.name(),
                        returnTypeName, paramTypeNames))) {
            mc.setModifiers(Modifier.PUBLIC);

            // Build args array
            ResultHandle argsArray = mc.newArray(Object.class, mc.load(method.parametersCount()));
            for (int i = 0; i < method.parametersCount(); i++) {
                ResultHandle param = mc.getMethodParam(i);
                Type paramType = method.parameterType(i);
                if (paramType.kind() == Type.Kind.PRIMITIVE) {
                    param = boxPrimitive(mc, param, paramType.asPrimitiveType());
                }
                mc.writeArrayValue(argsArray, i, param);
            }

            ResultHandle result = mc.invokeStaticMethod(
                    MethodDescriptor.ofMethod(
                            FindMethodBridge.class,
                            bridgeMethod,
                            bridgeReturnType,
                            AbstractMorphiumRepository.class,
                            String.class, String.class,
                            int.class, int.class, int.class, int.class,
                            Object[].class, boolean.class, boolean.class, boolean.class, boolean.class),
                    mc.getThis(),
                    mc.load(conditionsSpec.toString()),
                    mc.load(orderBySpec),
                    mc.load(sortParamIndex),
                    mc.load(orderParamIndex),
                    mc.load(pageRequestParamIndex),
                    mc.load(limitParamIndex),
                    argsArray,
                    mc.load(returnsSingle),
                    mc.load(returnsOptional),
                    mc.load(returnsCursoredPage),
                    mc.load(returnsStream));

            if (!isAsync && returnType.kind() == Type.Kind.PRIMITIVE) {
                result = unboxPrimitive(mc, result, returnType.asPrimitiveType());
            }

            mc.returnValue(result);
        }

        log.infof("Generated @Find method: %s.%s%s → find (conditions: %d, orderBy: %s)",
                method.declaringClass().name(), method.name(),
                isAsync ? " (async)" : "",
                conditionCount,
                orderBySpec.isEmpty() ? "none" : orderBySpec);
    }

    /**
     * Generates implementation for a {@code @Delete} annotated method.
     * If the method has {@code @By} parameters, deletes matching entities.
     * If the method has a single entity parameter, delegates to doDelete().
     */
    private void generateDeleteAnnotatedMethod(ClassCreator cc, MethodInfo method,
                                                String entityClassName) {
        // Check if this is entity-parameter delete or @By-condition delete
        boolean hasByParams = false;
        StringBuilder conditionsSpec = new StringBuilder();
        for (int i = 0; i < method.parametersCount(); i++) {
            AnnotationInstance byAnn = method.parameters().get(i).annotation(BY_ANNOTATION);
            if (byAnn != null) {
                hasByParams = true;
                String fieldName = byAnn.value().asString();
                if (conditionsSpec.length() > 0) conditionsSpec.append(",");
                conditionsSpec.append(fieldName).append(":").append(i);
            }
        }

        String[] paramTypeNames = new String[method.parametersCount()];
        for (int i = 0; i < method.parametersCount(); i++) {
            paramTypeNames[i] = toDescriptorName(method.parameterType(i));
        }
        String returnTypeName = toDescriptorName(method.returnType());

        if (hasByParams) {
            // Delete by conditions
            try (MethodCreator mc = cc.getMethodCreator(
                    MethodDescriptor.ofMethod(cc.getClassName(), method.name(),
                            returnTypeName, paramTypeNames))) {
                mc.setModifiers(Modifier.PUBLIC);

                ResultHandle argsArray = mc.newArray(Object.class, mc.load(method.parametersCount()));
                for (int i = 0; i < method.parametersCount(); i++) {
                    ResultHandle param = mc.getMethodParam(i);
                    Type paramType = method.parameterType(i);
                    if (paramType.kind() == Type.Kind.PRIMITIVE) {
                        param = boxPrimitive(mc, param, paramType.asPrimitiveType());
                    }
                    mc.writeArrayValue(argsArray, i, param);
                }

                mc.invokeStaticMethod(
                        MethodDescriptor.ofMethod(
                                FindMethodBridge.class,
                                "executeAnnotatedDelete",
                                void.class,
                                AbstractMorphiumRepository.class,
                                String.class, Object[].class),
                        mc.getThis(),
                        mc.load(conditionsSpec.toString()),
                        argsArray);
                mc.returnVoid();
            }
        } else {
            // Single entity parameter → delegate to doDelete
            try (MethodCreator mc = cc.getMethodCreator(
                    MethodDescriptor.ofMethod(cc.getClassName(), method.name(),
                            returnTypeName, paramTypeNames))) {
                mc.setModifiers(Modifier.PUBLIC);
                mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                                "doDelete", void.class, Object.class),
                        mc.getThis(), mc.getMethodParam(0));
                mc.returnVoid();
            }
        }

        log.infof("Generated @Delete method: %s.%s", method.declaringClass().name(), method.name());
    }

    /**
     * Generates implementation for an {@code @Insert} annotated method.
     * Delegates to doInsert() / doInsertAll().
     */
    private void generateInsertAnnotatedMethod(ClassCreator cc, MethodInfo method) {
        String[] paramTypeNames = new String[method.parametersCount()];
        for (int i = 0; i < method.parametersCount(); i++) {
            paramTypeNames[i] = toDescriptorName(method.parameterType(i));
        }
        String returnTypeName = toDescriptorName(method.returnType());
        boolean isList = isList(method.parameterType(0));

        try (MethodCreator mc = cc.getMethodCreator(
                MethodDescriptor.ofMethod(cc.getClassName(), method.name(),
                        returnTypeName, paramTypeNames))) {
            mc.setModifiers(Modifier.PUBLIC);

            if (isList) {
                ResultHandle result = mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                                "doInsertAll", List.class, List.class),
                        mc.getThis(), mc.getMethodParam(0));
                mc.returnValue(result);
            } else {
                ResultHandle result = mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                                "doInsert", Object.class, Object.class),
                        mc.getThis(), mc.getMethodParam(0));
                mc.returnValue(result);
            }
        }

        log.infof("Generated @Insert method: %s.%s", method.declaringClass().name(), method.name());
    }

    /**
     * Generates implementation for a {@code @Save} annotated method.
     * Delegates to doSave() / doSaveAll().
     */
    private void generateSaveAnnotatedMethod(ClassCreator cc, MethodInfo method) {
        String[] paramTypeNames = new String[method.parametersCount()];
        for (int i = 0; i < method.parametersCount(); i++) {
            paramTypeNames[i] = toDescriptorName(method.parameterType(i));
        }
        String returnTypeName = toDescriptorName(method.returnType());
        boolean isList = isList(method.parameterType(0));

        try (MethodCreator mc = cc.getMethodCreator(
                MethodDescriptor.ofMethod(cc.getClassName(), method.name(),
                        returnTypeName, paramTypeNames))) {
            mc.setModifiers(Modifier.PUBLIC);

            if (isList) {
                ResultHandle result = mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                                "doSaveAll", List.class, List.class),
                        mc.getThis(), mc.getMethodParam(0));
                mc.returnValue(result);
            } else {
                ResultHandle result = mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                                "doSave", Object.class, Object.class),
                        mc.getThis(), mc.getMethodParam(0));
                mc.returnValue(result);
            }
        }

        log.infof("Generated @Save method: %s.%s", method.declaringClass().name(), method.name());
    }

    /**
     * Generates implementation for an {@code @Update} annotated method.
     * Delegates to doUpdate() / doUpdateAll().
     */
    private void generateUpdateAnnotatedMethod(ClassCreator cc, MethodInfo method) {
        String[] paramTypeNames = new String[method.parametersCount()];
        for (int i = 0; i < method.parametersCount(); i++) {
            paramTypeNames[i] = toDescriptorName(method.parameterType(i));
        }
        String returnTypeName = toDescriptorName(method.returnType());
        boolean isList = isList(method.parameterType(0));

        try (MethodCreator mc = cc.getMethodCreator(
                MethodDescriptor.ofMethod(cc.getClassName(), method.name(),
                        returnTypeName, paramTypeNames))) {
            mc.setModifiers(Modifier.PUBLIC);

            if (isList) {
                ResultHandle result = mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                                "doUpdateAll", List.class, List.class),
                        mc.getThis(), mc.getMethodParam(0));
                mc.returnValue(result);
            } else {
                ResultHandle result = mc.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(AbstractMorphiumRepository.class,
                                "doUpdate", Object.class, Object.class),
                        mc.getThis(), mc.getMethodParam(0));
                mc.returnValue(result);
            }
        }

        log.infof("Generated @Update method: %s.%s", method.declaringClass().name(), method.name());
    }

    /**
     * Builds the orderBy spec string from {@code @OrderBy} annotations on a method.
     */
    private String buildOrderBySpec(MethodInfo method) {
        StringBuilder sb = new StringBuilder();

        // Check for @OrderBy.List (repeatable container)
        AnnotationInstance listAnn = method.annotation(ORDER_BY_LIST_ANNOTATION);
        if (listAnn != null) {
            for (AnnotationInstance orderBy : listAnn.value().asNestedArray()) {
                if (sb.length() > 0) sb.append(",");
                sb.append(orderBy.value().asString());
                sb.append(":");
                sb.append(isDescending(orderBy) ? "DESC" : "ASC");
            }
            return sb.toString();
        }

        // Check for single @OrderBy
        AnnotationInstance orderBy = method.annotation(ORDER_BY_ANNOTATION);
        if (orderBy != null) {
            sb.append(orderBy.value().asString());
            sb.append(":");
            sb.append(isDescending(orderBy) ? "DESC" : "ASC");
        }

        return sb.toString();
    }

    private boolean isDescending(AnnotationInstance orderBy) {
        var val = orderBy.value("descending");
        return val != null && val.asBoolean();
    }

    // -----------------------------------------------------------------
    // Phase 5: @Query with JDQL generation
    // -----------------------------------------------------------------

    /**
     * Generates implementation for a {@code @Query} annotated method.
     * Extracts the JDQL string and builds a {@code @Param} name-to-index mapping.
     * Special parameters (Sort, Order, PageRequest, Limit) are detected by type.
     */
    private void generateQueryAnnotatedMethod(ClassCreator cc, MethodInfo method,
                                               String entityClassName,
                                               IndexView index,
                                               BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        // Extract JDQL string from @Query annotation
        AnnotationInstance queryAnn = method.annotation(QUERY_ANNOTATION);
        String jdql = queryAnn.value().asString();

        // Build-time validation: reject MongoDB JSON syntax and positional parameters
        validateJdqlSyntax(jdql, method);

        // Build @Param name-to-index mapping and detect special params
        StringBuilder paramMapSpec = new StringBuilder();
        int sortParamIndex = -1;
        int orderParamIndex = -1;
        int pageRequestParamIndex = -1;
        int limitParamIndex = -1;

        List<MethodParameterInfo> params = method.parameters();
        for (int i = 0; i < method.parametersCount(); i++) {
            Type paramType = method.parameterType(i);
            DotName paramTypeName = paramType.name();

            // Check for special parameter types
            if (paramTypeName.equals(SORT_TYPE)) {
                sortParamIndex = i;
                continue;
            }
            if (paramTypeName.equals(ORDER_TYPE)) {
                orderParamIndex = i;
                continue;
            }
            if (paramTypeName.equals(PAGE_REQUEST_TYPE)) {
                pageRequestParamIndex = i;
                continue;
            }
            if (paramTypeName.equals(LIMIT_TYPE)) {
                limitParamIndex = i;
                continue;
            }

            // Check for @Param annotation; fall back to method parameter name
            // if compiled with -parameters (Jakarta Data spec §4.6.1)
            AnnotationInstance paramAnn = params.get(i).annotation(PARAM_ANNOTATION);
            String paramName = null;
            if (paramAnn != null) {
                paramName = paramAnn.value().asString();
            } else {
                String methodParamName = params.get(i).name();
                if (methodParamName != null) {
                    paramName = methodParamName;
                }
            }
            if (paramName != null) {
                if (paramMapSpec.length() > 0) paramMapSpec.append(",");
                paramMapSpec.append(paramName).append(":").append(i);
            }
        }

        // Build orderBy spec from @OrderBy annotations (used by CursoredPage)
        String orderBySpec = buildOrderBySpec(method);

        // Detect async: CompletionStage<X> → unwrap X as effective return type
        Type returnType = method.returnType();
        boolean isAsync = isCompletionStage(returnType);
        Type effectiveReturnType = isAsync ? unwrapCompletionStage(returnType) : returnType;

        // Determine return type characteristics (based on effective/inner type)
        boolean returnsOptional = isOptional(effectiveReturnType);
        boolean returnsCursoredPage = effectiveReturnType.name().equals(CURSORED_PAGE_TYPE);
        boolean returnsStream = isStream(effectiveReturnType);
        boolean returnsSingle = !isList(effectiveReturnType) && !returnsStream
                && !returnsOptional
                && !effectiveReturnType.name().equals(PAGE_TYPE)
                && !returnsCursoredPage;
        boolean returnsCount = effectiveReturnType.kind() == Type.Kind.PRIMITIVE
                && effectiveReturnType.asPrimitiveType().primitive() == PrimitiveType.Primitive.LONG;
        boolean returnsBoolean = effectiveReturnType.kind() == Type.Kind.PRIMITIVE
                && effectiveReturnType.asPrimitiveType().primitive() == PrimitiveType.Primitive.BOOLEAN;

        // Detect Record return type for GROUP BY support
        String resultRecordClass = null;
        if (isList(effectiveReturnType) && effectiveReturnType.kind() == Type.Kind.PARAMETERIZED_TYPE) {
            Type innerType = effectiveReturnType.asParameterizedType().arguments().get(0);
            DotName innerTypeName = innerType.name();
            if (!innerTypeName.toString().equals(entityClassName)) {
                ClassInfo innerClassInfo = index.getClassByName(innerTypeName);
                if (innerClassInfo != null
                        && innerClassInfo.superName() != null
                        && innerClassInfo.superName().toString().equals("java.lang.Record")) {
                    resultRecordClass = innerTypeName.toString();
                    reflectiveClasses.produce(
                            ReflectiveClassBuildItem.builder(resultRecordClass)
                                    .constructors(true).methods(true).build());
                }
            }
        }

        // Also detect Record for Page<Record> return types (GROUP BY pagination)
        if (resultRecordClass == null
                && effectiveReturnType.name().equals(PAGE_TYPE)
                && effectiveReturnType.kind() == Type.Kind.PARAMETERIZED_TYPE) {
            Type innerType = effectiveReturnType.asParameterizedType().arguments().get(0);
            DotName innerTypeName = innerType.name();
            if (!innerTypeName.toString().equals(entityClassName)) {
                ClassInfo innerClassInfo = index.getClassByName(innerTypeName);
                if (innerClassInfo != null
                        && innerClassInfo.superName() != null
                        && innerClassInfo.superName().toString().equals("java.lang.Record")) {
                    resultRecordClass = innerTypeName.toString();
                    reflectiveClasses.produce(
                            ReflectiveClassBuildItem.builder(resultRecordClass)
                                    .constructors(true).methods(true).build());
                }
            }
        }

        // Build parameter type descriptors
        String[] paramTypeNames = new String[method.parametersCount()];
        for (int i = 0; i < method.parametersCount(); i++) {
            paramTypeNames[i] = toDescriptorName(method.parameterType(i));
        }
        String returnTypeName = toDescriptorName(returnType);

        String bridgeMethod = isAsync ? "executeJdqlAsync" : "executeJdql";
        Class<?> bridgeReturnType = isAsync ? CompletionStage.class : Object.class;

        try (MethodCreator mc = cc.getMethodCreator(
                MethodDescriptor.ofMethod(cc.getClassName(), method.name(),
                        returnTypeName, paramTypeNames))) {
            mc.setModifiers(Modifier.PUBLIC);

            // Build args array
            ResultHandle argsArray = mc.newArray(Object.class, mc.load(method.parametersCount()));
            for (int i = 0; i < method.parametersCount(); i++) {
                ResultHandle param = mc.getMethodParam(i);
                Type paramType = method.parameterType(i);
                if (paramType.kind() == Type.Kind.PRIMITIVE) {
                    param = boxPrimitive(mc, param, paramType.asPrimitiveType());
                }
                mc.writeArrayValue(argsArray, i, param);
            }

            ResultHandle result = mc.invokeStaticMethod(
                    MethodDescriptor.ofMethod(
                            JdqlMethodBridge.class,
                            bridgeMethod,
                            bridgeReturnType,
                            AbstractMorphiumRepository.class,
                            String.class, String.class,
                            int.class, int.class, int.class, int.class,
                            Object[].class,
                            boolean.class, boolean.class, boolean.class, boolean.class,
                            boolean.class, String.class, boolean.class,
                            String.class),
                    mc.getThis(),
                    mc.load(jdql),
                    mc.load(paramMapSpec.toString()),
                    mc.load(sortParamIndex),
                    mc.load(orderParamIndex),
                    mc.load(pageRequestParamIndex),
                    mc.load(limitParamIndex),
                    argsArray,
                    mc.load(returnsSingle),
                    mc.load(returnsCount),
                    mc.load(returnsBoolean),
                    mc.load(returnsOptional),
                    mc.load(returnsCursoredPage),
                    mc.load(orderBySpec),
                    mc.load(returnsStream),
                    resultRecordClass != null ? mc.load(resultRecordClass) : mc.loadNull());

            // Unbox primitive return types (skip for async — returns CompletionStage)
            if (!isAsync && returnType.kind() == Type.Kind.PRIMITIVE) {
                result = unboxPrimitive(mc, result, returnType.asPrimitiveType());
            }

            mc.returnValue(result);
        }

        log.infof("Generated @Query method: %s.%s%s → JDQL: %s", method.declaringClass().name(), method.name(),
                isAsync ? " (async)" : "", jdql == null || jdql.isBlank() ? "(no filter / find all)" : jdql);
    }

    /**
     * Validates that a {@code @Query} annotation value uses JDQL syntax with named parameters
     * ({@code :paramName}), not MongoDB JSON syntax or JPA-style positional parameters ({@code ?1}).
     *
     * @throws IllegalStateException if the query uses unsupported syntax
     */
    private void validateJdqlSyntax(String jdql, MethodInfo method) {
        if (jdql == null || jdql.isBlank()) {
            return;
        }
        String trimmed = jdql.trim();
        // Detect MongoDB JSON syntax: starts with { or contains $-operators
        if (trimmed.startsWith("{")) {
            throw new IllegalStateException(
                    "@Query on " + method.declaringClass().name() + "." + method.name()
                    + " uses MongoDB JSON syntax: \"" + jdql + "\". "
                    + "Jakarta Data @Query requires JDQL syntax with named parameters (:paramName). "
                    + "Example: @Query(\"WHERE field = :param AND other >= :min\")");
        }
        // Detect JPA-style positional parameters: ?1, ?2, etc.
        if (trimmed.matches(".*\\?\\d+.*")) {
            throw new IllegalStateException(
                    "@Query on " + method.declaringClass().name() + "." + method.name()
                    + " uses positional parameters (?1, ?2, ...): \"" + jdql + "\". "
                    + "Jakarta Data @Query requires named parameters (:paramName). "
                    + "Example: @Query(\"WHERE field = :param\") with @Param(\"param\") on method parameters.");
        }
    }

    // -----------------------------------------------------------------
    // Type resolution helpers
    // -----------------------------------------------------------------

    private record TypeParameters(DotName entityType, DotName idType) {}

    private TypeParameters resolveEntityAndIdTypes(ClassInfo repoClass, IndexView index) {
        for (Type superInterface : repoClass.interfaceTypes()) {
            TypeParameters result = resolveFromType(superInterface, index);
            if (result != null) return result;
        }
        return null;
    }

    private TypeParameters resolveFromType(Type type, IndexView index) {
        if (type.kind() == Type.Kind.PARAMETERIZED_TYPE) {
            ParameterizedType pt = type.asParameterizedType();
            DotName name = pt.name();
            if (name.equals(BASIC_REPOSITORY) || name.equals(CRUD_REPOSITORY)
                    || name.equals(DATA_REPOSITORY) || name.equals(MORPHIUM_REPOSITORY)) {
                if (pt.arguments().size() >= 2) {
                    DotName entityType = pt.arguments().get(0).name();
                    DotName idType = pt.arguments().get(1).name();
                    return new TypeParameters(entityType, idType);
                }
            }
            ClassInfo ci = index.getClassByName(name);
            if (ci != null) {
                for (Type si : ci.interfaceTypes()) {
                    TypeParameters result = resolveFromType(
                            resolveTypeArgs(si, ci.typeParameters(), pt.arguments()), index);
                    if (result != null) return result;
                }
            }
        } else if (type.kind() == Type.Kind.CLASS) {
            ClassInfo ci = index.getClassByName(type.name());
            if (ci != null) {
                for (Type si : ci.interfaceTypes()) {
                    TypeParameters result = resolveFromType(si, index);
                    if (result != null) return result;
                }
            }
        }
        return null;
    }

    private Type resolveTypeArgs(Type type, List<TypeVariable> typeParams, List<Type> actualArgs) {
        if (type.kind() == Type.Kind.TYPE_VARIABLE) {
            String varName = type.asTypeVariable().identifier();
            for (int i = 0; i < typeParams.size(); i++) {
                if (typeParams.get(i).identifier().equals(varName) && i < actualArgs.size()) {
                    return actualArgs.get(i);
                }
            }
        } else if (type.kind() == Type.Kind.PARAMETERIZED_TYPE) {
            ParameterizedType pt = type.asParameterizedType();
            List<Type> resolvedArgs = new ArrayList<>();
            boolean changed = false;
            for (Type arg : pt.arguments()) {
                Type resolved = resolveTypeArgs(arg, typeParams, actualArgs);
                resolvedArgs.add(resolved);
                if (resolved != arg) changed = true;
            }
            if (changed) {
                return ParameterizedType.create(pt.name(), resolvedArgs.toArray(new Type[0]), null);
            }
        }
        return type;
    }

    private String findIdField(ClassInfo entityClass, IndexView index) {
        ClassInfo current = entityClass;
        while (current != null) {
            for (FieldInfo field : current.fields()) {
                if (field.hasAnnotation(ID_ANNOTATION)) {
                    return field.name();
                }
            }
            DotName superName = current.superName();
            if (superName == null || superName.toString().equals("java.lang.Object")) break;
            current = index.getClassByName(superName);
        }
        return null;
    }

    private boolean implementsInterface(ClassInfo classInfo, DotName interfaceName, IndexView index) {
        if (classInfo == null) return false;
        for (Type si : classInfo.interfaceTypes()) {
            DotName name = si.name();
            if (name.equals(interfaceName)) return true;
            ClassInfo siClass = index.getClassByName(name);
            if (siClass != null && implementsInterface(siClass, interfaceName, index)) return true;
        }
        return false;
    }

    private Set<String> collectEntityFields(ClassInfo entityClass, IndexView index) {
        Set<String> fields = new LinkedHashSet<>();
        ClassInfo current = entityClass;
        while (current != null) {
            for (FieldInfo field : current.fields()) {
                if (!Modifier.isStatic(field.flags())
                        && !Modifier.isTransient(field.flags())) {
                    fields.add(field.name());
                }
            }
            DotName superName = current.superName();
            if (superName == null || superName.toString().equals("java.lang.Object")) break;
            current = index.getClassByName(superName);
        }
        return fields;
    }

    // -- Return type analysis --

    private boolean isList(Type type) {
        return type.name().toString().equals("java.util.List");
    }

    private boolean isStream(Type type) {
        return type.name().toString().equals("java.util.stream.Stream");
    }

    private boolean isOptional(Type type) {
        return type.name().toString().equals("java.util.Optional");
    }

    private boolean isCompletionStage(Type type) {
        return type.name().equals(COMPLETION_STAGE_TYPE);
    }

    /**
     * If the type is {@code CompletionStage<X>}, returns the inner type X.
     * Otherwise returns null.
     */
    private Type unwrapCompletionStage(Type type) {
        if (!isCompletionStage(type)) return null;
        if (type.kind() == Type.Kind.PARAMETERIZED_TYPE) {
            return type.asParameterizedType().arguments().get(0);
        }
        return null;
    }

    // -- Generic signature builder --

    private String buildGenericSignature(String superClass, String interfaceName,
                                         String entityClass, String idClass) {
        String entityDesc = "L" + entityClass.replace('.', '/') + ";";
        String idDesc = "L" + idClass.replace('.', '/') + ";";
        String superDesc = "L" + superClass.replace('.', '/') + "<" + entityDesc + idDesc + ">;";
        String ifaceDesc = "L" + interfaceName.replace('.', '/') + ";";
        return superDesc + ifaceDesc;
    }
}
