package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;

/**
 * AspectJ aspect that wraps every method (or every method of every class) annotated
 * with {@code @}{@link MorphiumTransactional} in a Morphium transaction:
 * {@code startTransaction()} before the method runs, {@code commitTransaction()} on
 * normal return, {@code abortTransaction()} if the method throws.
 *
 * <p>Registered as a plain {@code @Component}, so it only becomes an active Spring
 * bean — and only then does its {@code @Around} advice apply — when both hold:
 * <ul>
 *   <li>{@code org.aspectj.lang.annotation.Aspect} is on the classpath
 *       ({@code @ConditionalOnClass(name = "org.aspectj.lang.annotation.Aspect")}) —
 *       i.e. {@code spring-boot-starter-aop} (an optional dependency of this module)
 *       is present;</li>
 *   <li>a {@link Morphium} bean already exists in the context
 *       ({@code @ConditionalOnBean}).</li>
 * </ul>
 * Unlike the {@code @AutoConfiguration} classes in this package, this class is a
 * plain {@code @Component} picked up by Spring Boot's component scan (or explicit
 * bean registration) rather than the auto-configuration import mechanism — but the
 * two {@code @Conditional} annotations are evaluated the same way.
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
@Component
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
