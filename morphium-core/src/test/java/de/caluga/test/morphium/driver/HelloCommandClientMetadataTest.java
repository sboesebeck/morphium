package de.caluga.test.morphium.driver;

import de.caluga.morphium.MorphiumVersion;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.test.ConnectionMock;
import de.caluga.test.DriverMock;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class HelloCommandClientMetadataTest {

    @Test
    public void versionIsResolvedFromBuild() {
        String version = MorphiumVersion.getVersion();
        assertNotNull(version);
        assertNotEquals(MorphiumVersion.UNKNOWN_VERSION, version);
        assertFalse(version.contains("${"), "maven resource filtering did not run: " + version);
        assertTrue(version.matches("\\d+\\..*"), "not a version number: " + version);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void helloSendsRealVersionAndDefaultAppName() {
        HelloCommand cmd = new HelloCommand(new ConnectionMock());
        Map<String, Object> map = cmd.asMap();
        Map<String, Object> client = (Map<String, Object>) map.get("client");
        assertNotNull(client);
        Map<String, Object> driver = (Map<String, Object>) client.get("driver");
        assertEquals(MorphiumVersion.getVersion(), driver.get("version"));
        assertEquals("Morphium/mock", driver.get("name"));
        Map<String, Object> application = (Map<String, Object>) client.get("application");
        assertEquals("Morphium", application.get("name"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void helloSendsConfiguredAppName() {
        ConnectionMock con = new ConnectionMock() {
            private final MorphiumDriver drv = new DriverMock() {
                @Override
                public String getAppName() {
                    return "myService";
                }
            };
            @Override
            public MorphiumDriver getDriver() {
                return drv;
            }
        };
        HelloCommand cmd = new HelloCommand(con);
        Map<String, Object> map = cmd.asMap();
        Map<String, Object> client = (Map<String, Object>) map.get("client");
        Map<String, Object> application = (Map<String, Object>) client.get("application");
        assertEquals("myService", application.get("name"));
    }

    @Test
    public void noClientMetadataWhenDisabled() {
        HelloCommand cmd = new HelloCommand(new ConnectionMock());
        cmd.setIncludeClient(false);
        assertFalse(cmd.asMap().containsKey("client"));
    }
}
