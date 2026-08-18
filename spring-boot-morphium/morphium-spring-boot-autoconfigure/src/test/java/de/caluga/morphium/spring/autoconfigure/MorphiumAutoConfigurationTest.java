package de.caluga.morphium.spring.autoconfigure;

import de.caluga.morphium.Morphium;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("test")
class MorphiumAutoConfigurationTest {

    @Autowired
    Morphium morphium;

    @Test
    void morphiumBeanIsCreated() {
        assertNotNull(morphium);
        assertEquals("test", morphium.getConfig().connectionSettings().getDatabase());
    }

    @Test
    void driverIsInMemory() {
        assertTrue(morphium.getDriver().getClass().getSimpleName().contains("InMem"),
                "Expected InMemDriver but got: " + morphium.getDriver().getClass().getName());
    }
}
