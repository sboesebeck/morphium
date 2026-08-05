package de.caluga.morphium.spring.autoconfigure;

import java.lang.annotation.*;

/**
 * Marks a method, or every method of a class, to run inside a Morphium transaction.
 * {@link MorphiumTransactionAspect} intercepts every call to an annotated element,
 * calling {@code Morphium.startTransaction()} beforehand and, depending on outcome,
 * either {@code commitTransaction()} (normal return) or {@code abortTransaction()}
 * (any thrown exception) afterwards — the caller does not manage the transaction
 * manually.
 *
 * <p>Requires a MongoDB replica set or Atlas cluster
 * ({@code morphium.replica-set-name}) — single-node standalone MongoDB does not
 * support multi-document transactions. Requires {@code spring-boot-starter-aop} on
 * the classpath for the aspect to be woven in; see {@link MorphiumTransactionAspect}
 * for the exact activation conditions.
 *
 * <pre>{@code
 * @MorphiumTransactional
 * public void placeOrder(Order order, Payment payment) {
 *     morphium.store(order);
 *     morphium.store(payment);
 * }
 * }</pre>
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MorphiumTransactional {
}
