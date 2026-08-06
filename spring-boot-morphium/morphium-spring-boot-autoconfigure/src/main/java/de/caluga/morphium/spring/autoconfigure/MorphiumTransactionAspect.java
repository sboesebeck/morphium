package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;

/**
 * AspectJ aspect that wraps every method (or every method of every class) annotated
 * with {@code @}{@link MorphiumTransactional} in a Morphium transaction:
 * {@code startTransaction()} before the method runs, {@code commitTransaction()} on
 * normal return, {@code abortTransaction()} if the method throws.
 *
 * <p>Registered as {@code @AutoConfiguration} (not a plain {@code @Component}
 * picked up by component scan — see the "why" note below), so it only becomes an
 * active Spring bean — and only then does its {@code @Around} advice apply — when
 * both hold:
 * <ul>
 *   <li>{@code org.aspectj.lang.annotation.Aspect} is on the classpath
 *       ({@code @ConditionalOnClass(name = "org.aspectj.lang.annotation.Aspect")}) —
 *       i.e. {@code spring-boot-starter-aop} (an optional dependency of this module)
 *       is present;</li>
 *   <li>a {@link Morphium} bean already exists in the context
 *       ({@code @ConditionalOnBean}).</li>
 * </ul>
 * <p><b>Why {@code @AutoConfiguration} and not {@code @Component}:</b> this class
 * lives in {@code de.caluga.morphium.spring.autoconfigure}, a package that belongs to
 * this library, not to any application using it. Spring Boot's component scan only
 * looks at the application's own base package (and its sub-packages) unless told
 * otherwise, so a plain {@code @Component} here is picked up only by coincidence —
 * for any real application depending on this starter as an external jar, it is
 * simply never scanned, silently leaving {@code @MorphiumTransactional} methods
 * running without a transaction. Registering this class in
 * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}
 * (alongside {@link MorphiumAutoConfiguration} and
 * {@link MorphiumHealthAutoConfiguration}) makes Spring Boot's auto-configuration
 * import mechanism instantiate it regardless of the application's package structure.</p>
 *
 * <p>Requires a MongoDB replica set or Atlas cluster
 * ({@code morphium.replica-set-name}) — a standalone MongoDB node rejects
 * multi-document transactions.
 *
 * <pre>{@code
 * @Service
 * public class OrderService {
 *     @Autowired Morphium morphium;
 *
 *     @MorphiumTransactional
 *     public void placeOrder(Order order, Payment payment) {
 *         morphium.store(order);
 *         morphium.store(payment);
 *         // committed automatically on return, rolled back automatically on exception
 *     }
 * }
 * }</pre>
 */
@Aspect
@AutoConfiguration
@ConditionalOnClass(name = "org.aspectj.lang.annotation.Aspect")
@ConditionalOnBean(Morphium.class)
public class MorphiumTransactionAspect {

    private final Morphium morphium;

    /**
     * Creates the aspect bound to the application's single {@link Morphium} instance.
     * Instantiated by Spring, not application code — see the class-level
     * {@code @Conditional} documentation for when this happens.
     *
     * @param morphium the {@link Morphium} bean every advised method's transaction is
     *                 started, committed, or aborted on
     */
    public MorphiumTransactionAspect(Morphium morphium) {
        this.morphium = morphium;
    }

    /**
     * Advice applied around any method annotated with {@code @MorphiumTransactional},
     * or any method of a class annotated with it. Calls
     * {@code morphium.startTransaction()} before {@code pjp.proceed()}; on normal
     * completion, calls {@code commitTransaction()} and returns the method's result
     * unchanged; if {@code pjp.proceed()} throws anything, calls
     * {@code abortTransaction()} and rethrows the original exception unchanged.
     *
     * @param pjp the join point representing the intercepted method invocation
     * @return whatever the advised method returned
     * @throws Throwable whatever the advised method threw, after the transaction has
     *         been aborted
     */
    @Around("@annotation(de.caluga.morphium.spring.autoconfigure.MorphiumTransactional) || " +
            "@within(de.caluga.morphium.spring.autoconfigure.MorphiumTransactional)")
    public Object aroundTransactional(ProceedingJoinPoint pjp) throws Throwable {
        morphium.startTransaction();
        try {
            Object result = pjp.proceed();
            morphium.commitTransaction();
            return result;
        } catch (Throwable t) {
            morphium.abortTransaction();
            throw t;
        }
    }
}
