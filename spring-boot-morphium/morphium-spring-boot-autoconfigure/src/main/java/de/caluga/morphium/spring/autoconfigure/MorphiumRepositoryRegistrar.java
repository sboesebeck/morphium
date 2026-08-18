package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.data.MorphiumRepository;
import jakarta.data.repository.CrudRepository;
import jakarta.data.repository.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link ImportBeanDefinitionRegistrar} that performs the actual classpath scan
 * behind {@link EnableMorphiumRepositories}: it looks for interfaces annotated with
 * {@code jakarta.data.repository.Repository} that extend {@link CrudRepository} or
 * {@link MorphiumRepository}, and registers one {@link MorphiumRepositoryFactoryBean}
 * bean definition per interface found. Never referenced directly by application
 * code — Spring instantiates and invokes it automatically because
 * {@code @EnableMorphiumRepositories} carries {@code @Import(MorphiumRepositoryRegistrar.class)}.
 *
 * <p>This is where this module's proxy mechanism differs architecturally from the
 * {@code quarkus-morphium} extension: this class runs at Spring context-startup time,
 * in the running JVM, and only ever registers a {@link MorphiumRepositoryFactoryBean}
 * — a {@code FactoryBean} that later produces a
 * {@link java.lang.reflect.Proxy JDK dynamic proxy}. No bytecode is generated or
 * written to disk. Quarkus's equivalent mechanism runs as a build-time processor and
 * emits a real, compiled implementation class via Gizmo before the application ever
 * starts. See {@link EnableMorphiumRepositories} for the full comparison.
 */
public class MorphiumRepositoryRegistrar implements ImportBeanDefinitionRegistrar {

    private static final Logger log = LoggerFactory.getLogger(MorphiumRepositoryRegistrar.class);

    /**
     * Scans the base packages derived from {@code @EnableMorphiumRepositories} (see
     * {@link #getBasePackages(AnnotationMetadata)}) for interfaces annotated with
     * {@code @Repository} that also extend {@link CrudRepository} or
     * {@link MorphiumRepository}, and registers a {@link MorphiumRepositoryFactoryBean}
     * bean definition — constructed with the repository interface as its sole
     * constructor argument and wired by type — for each match. The registered bean
     * name is the uncapitalized simple interface name (e.g. {@code ProductRepository}
     * becomes {@code productRepository}). Candidates that fail to load are logged as
     * a warning and skipped; interfaces that are annotated {@code @Repository} but do
     * not extend either supported base interface are silently skipped.
     *
     * @param importingClassMetadata metadata of the class carrying
     *                               {@code @EnableMorphiumRepositories}, used to read
     *                               its {@code value()}/{@code basePackages()}
     *                               attributes
     * @param registry the bean definition registry to register discovered repository
     *                 factory beans into
     */
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata,
                                        BeanDefinitionRegistry registry) {
        Set<String> basePackages = getBasePackages(importingClassMetadata);
        if (basePackages.isEmpty()) {
            return;
        }

        var scanner = new ClassPathScanningCandidateComponentProvider(false) {
            @Override
            protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
                // Allow interfaces (default implementation rejects them)
                return beanDefinition.getMetadata().isInterface()
                        && beanDefinition.getMetadata().isIndependent();
            }
        };
        scanner.addIncludeFilter(new AnnotationTypeFilter(Repository.class));

        for (String basePackage : basePackages) {
            for (var candidate : scanner.findCandidateComponents(basePackage)) {
                String className = candidate.getBeanClassName();
                if (className == null) continue;

                try {
                    Class<?> iface = ClassUtils.forName(className, getClass().getClassLoader());
                    if (!iface.isInterface()) continue;
                    if (!CrudRepository.class.isAssignableFrom(iface)
                            && !MorphiumRepository.class.isAssignableFrom(iface)) {
                        continue;
                    }

                    String beanName = StringUtils.uncapitalize(iface.getSimpleName());

                    var bd = BeanDefinitionBuilder.genericBeanDefinition(MorphiumRepositoryFactoryBean.class)
                            .addConstructorArgValue(iface)
                            .setAutowireMode(AbstractBeanDefinition.AUTOWIRE_BY_TYPE)
                            .getBeanDefinition();

                    registry.registerBeanDefinition(beanName, bd);
                    log.debug("Registered Morphium repository bean '{}' for {}", beanName, className);
                } catch (ClassNotFoundException e) {
                    log.warn("Could not load repository candidate class: {}", className);
                }
            }
        }
    }

    private Set<String> getBasePackages(AnnotationMetadata metadata) {
        Set<String> packages = new HashSet<>();

        var attrs = AnnotationAttributes.fromMap(
                metadata.getAnnotationAttributes(EnableMorphiumRepositories.class.getName()));

        if (attrs != null) {
            packages.addAll(Arrays.asList(attrs.getStringArray("value")));
            packages.addAll(Arrays.asList(attrs.getStringArray("basePackages")));
        }

        if (packages.isEmpty()) {
            packages.add(ClassUtils.getPackageName(metadata.getClassName()));
        }

        return packages;
    }
}
