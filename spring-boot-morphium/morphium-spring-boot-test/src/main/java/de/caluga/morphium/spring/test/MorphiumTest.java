package de.caluga.morphium.spring.test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.lang.annotation.*;

/**
 * Composite test annotation that configures a Spring Boot test with the Morphium
 * in-memory driver. No MongoDB instance required.
 *
 * <pre>
 * {@code @MorphiumTest}
 * class MyRepositoryTest {
 *     {@code @Autowired} MyRepository repo;
 * }
 * </pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@SpringBootTest
@TestPropertySource(properties = {
        "morphium.database=test",
        "morphium.driver-name=InMemDriver",
        "morphium.hosts=localhost:27017"
})
public @interface MorphiumTest {
}
