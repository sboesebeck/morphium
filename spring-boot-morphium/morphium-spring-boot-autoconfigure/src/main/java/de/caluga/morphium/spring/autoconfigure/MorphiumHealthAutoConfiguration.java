package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration that registers a Spring Boot Actuator {@link HealthIndicator}
 * reporting the connection status of the application's {@link Morphium} bean under
 * {@code /actuator/health}. It runs after {@link MorphiumAutoConfiguration}
 * ({@code @AutoConfiguration(after = MorphiumAutoConfiguration.class)}) and applies
 * only when all of the following hold:
 * <ul>
 *   <li>{@code org.springframework.boot.actuate.health.HealthIndicator} is on the
 *       classpath ({@code @ConditionalOnClass}) — i.e. {@code spring-boot-actuator}
 *       is present;</li>
 *   <li>a {@link Morphium} bean already exists in the context
 *       ({@code @ConditionalOnBean}) — there is nothing to report on otherwise.</li>
 * </ul>
 *
 * <p>With both conditions met and no further configuration, {@code /actuator/health}
 * includes:
 *
 * <pre>{@code
 * {
 *   "components": {
 *     "morphium": {
 *       "status": "UP",
 *       "details": {
 *         "database": "my-database",
 *         "driver": "PooledDriver",
 *         "replicaSet": true,
 *         "replicaSetName": "rs0"
 *       }
 *     }
 *   }
 * }
 * }</pre>
 *
 * <h2>Disabling or overriding the indicator</h2>
 * The bean method is additionally guarded by
 * {@code @ConditionalOnEnabledHealthIndicator("morphium")} — set
 * {@code management.health.morphium.enabled=false} to turn it off entirely — and by
 * {@code @ConditionalOnMissingBean(name = "morphiumHealthIndicator")}, so defining
 * your own {@code @Bean(name = "morphiumHealthIndicator") HealthIndicator} in the
 * application context takes precedence over the auto-configured one.
 */
@AutoConfiguration(after = MorphiumAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
@ConditionalOnBean(Morphium.class)
public class MorphiumHealthAutoConfiguration {

    /**
     * Builds the {@code morphium} health indicator. On each invocation it checks
     * {@code morphium.getDriver().isConnected()} and reports {@code UP}/{@code DOWN}
     * accordingly, attaching the configured database name, driver name, and replica
     * set status/name as detail fields. Any exception thrown while querying the
     * driver is caught and reported as {@code DOWN} with the exception attached.
     *
     * <p>Guarded by {@code @ConditionalOnEnabledHealthIndicator("morphium")} (respects
     * {@code management.health.morphium.enabled}) and
     * {@code @ConditionalOnMissingBean(name = "morphiumHealthIndicator")} so a
     * user-defined bean of the same name overrides this one instead of colliding with
     * it.
     *
     * @param morphium the application's {@link Morphium} bean, guaranteed present by
     *                 {@code @ConditionalOnBean(Morphium.class)} on the class
     * @return a {@link HealthIndicator} reporting live connection status on every
     *         health check invocation
     */
    @Bean
    @ConditionalOnEnabledHealthIndicator("morphium")
    @ConditionalOnMissingBean(name = "morphiumHealthIndicator")
    public HealthIndicator morphiumHealthIndicator(Morphium morphium) {
        return () -> {
            try {
                var driver = morphium.getDriver();
                boolean connected = driver.isConnected();
                var builder = connected ? Health.up() : Health.down();
                builder.withDetail("database", morphium.getConfig().connectionSettings().getDatabase());
                builder.withDetail("driver", morphium.getConfig().driverSettings().getDriverName());
                builder.withDetail("replicaSet", driver.isReplicaSet());
                if (driver.getReplicaSetName() != null) {
                    builder.withDetail("replicaSetName", driver.getReplicaSetName());
                }
                return builder.build();
            } catch (Exception e) {
                return Health.down(e).build();
            }
        };
    }
}
