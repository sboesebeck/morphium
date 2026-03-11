package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.BackendType;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wire.PooledDriver;
import de.caluga.test.mongo.suite.inmem.MorphiumInMemTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("core")
public class CosmosDBDetectionTest extends MorphiumInMemTestBase {

    @Test
    public void testInMemoryDriverBackendType() {
        assertEquals(BackendType.IN_MEMORY, morphium.getBackendType());
        assertTrue(morphium.getDriver().isInMemoryBackend());
        assertFalse(morphium.getDriver().isCosmosDB());
    }

    @Test
    public void testDefaultDriverNotCosmosDB() {
        // InMemoryDriver should not be CosmosDB
        assertFalse(morphium.getDriver().isCosmosDB());
        assertEquals(BackendType.IN_MEMORY, morphium.getDriver().getBackendType());
    }

    @Test
    public void testBackendTypeEnumDerivation() {
        // Test the default method logic by mocking the flags
        // InMemory takes priority
        var driver = new InMemoryDriver();
        assertEquals(BackendType.IN_MEMORY, driver.getBackendType());
        assertTrue(driver.isInMemoryBackend());
    }

    @Test
    public void testPooledDriverCosmosDBHostDetection() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("myaccount.mongo.cosmos.azure.com:10255");

        // Use reflection to invoke the private detectCosmosDB method
        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();
        hello.setSetName("globaldb");

        // Signal 1: hostname match
        boolean result = (boolean) detectMethod.invoke(driver, hello, "myaccount.mongo.cosmos.azure.com:10255");
        assertTrue(result, "Should detect CosmosDB from hostname");

        // Non-CosmosDB host should not match
        boolean nonCosmos = (boolean) detectMethod.invoke(driver, hello, "localhost:27017");
        // But seed hosts contain cosmos host, so Signal 2 should still match
        assertTrue(nonCosmos, "Should detect CosmosDB from seed hosts");
    }

    @Test
    public void testPooledDriverCosmosDBVCoreHostDetection() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("mycluster.mongocluster.cosmos.azure.com:10260");

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();

        boolean result = (boolean) detectMethod.invoke(driver, hello, "mycluster.mongocluster.cosmos.azure.com:10260");
        assertTrue(result, "Should detect CosmosDB vCore from hostname");
    }

    @Test
    public void testPooledDriverGlobaldbSSLFallback() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("somehost:27017");
        driver.setUseSSL(true);

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();
        hello.setSetName("globaldb");

        boolean result = (boolean) detectMethod.invoke(driver, hello, "somehost:27017");
        assertTrue(result, "Should detect CosmosDB from setName=globaldb + SSL");
    }

    @Test
    public void testPooledDriverGlobaldbWithoutSSLNotCosmosDB() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("somehost:27017");
        driver.setUseSSL(false);

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();
        hello.setSetName("globaldb");

        boolean result = (boolean) detectMethod.invoke(driver, hello, "somehost:27017");
        assertFalse(result, "globaldb without SSL should not be detected as CosmosDB");
    }

    @Test
    public void testNormalMongoDBNotDetectedAsCosmosDB() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("mongo1:27017", "mongo2:27017", "mongo3:27017");

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();
        hello.setSetName("rs0");

        boolean result = (boolean) detectMethod.invoke(driver, hello, "mongo1:27017");
        assertFalse(result, "Normal MongoDB should not be detected as CosmosDB");
    }

    @Test
    public void testPooledDriverAzureChinaDetection() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("myaccount.mongo.cosmos.azure.cn:10255");

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();

        // RU API (China)
        boolean result = (boolean) detectMethod.invoke(driver, hello, "myaccount.mongo.cosmos.azure.cn:10255");
        assertTrue(result, "Should detect CosmosDB from Azure China RU API hostname");
    }

    @Test
    public void testPooledDriverAzureChinaVCoreDetection() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("mycluster.mongocluster.cosmos.azure.cn:10260");

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();

        boolean result = (boolean) detectMethod.invoke(driver, hello, "mycluster.mongocluster.cosmos.azure.cn:10260");
        assertTrue(result, "Should detect CosmosDB from Azure China vCore hostname");
    }

    @Test
    public void testPooledDriverAzureUSGovDetection() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("myaccount.mongo.cosmos.azure.us:10255");

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();

        boolean result = (boolean) detectMethod.invoke(driver, hello, "myaccount.mongo.cosmos.azure.us:10255");
        assertTrue(result, "Should detect CosmosDB from Azure US Gov RU API hostname");
    }

    @Test
    public void testPooledDriverAzureUSGovVCoreDetection() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("mycluster.mongocluster.cosmos.usgovcloudapi.net:10260");

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();

        boolean result = (boolean) detectMethod.invoke(driver, hello, "mycluster.mongocluster.cosmos.usgovcloudapi.net:10260");
        assertTrue(result, "Should detect CosmosDB from Azure US Gov vCore hostname");
    }

    @Test
    public void testPooledDriverAzureGermanyDetection() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("myaccount.mongo.cosmos.microsoftazure.de:10255");

        Method detectMethod = PooledDriver.class.getDeclaredMethod("detectCosmosDB", HelloResult.class, String.class);
        detectMethod.setAccessible(true);

        HelloResult hello = new HelloResult();

        boolean result = (boolean) detectMethod.invoke(driver, hello, "myaccount.mongo.cosmos.microsoftazure.de:10255");
        assertTrue(result, "Should detect CosmosDB from Azure Germany hostname");
    }

    @Test
    public void testBackendTypeEnumValues() {
        // Ensure all expected values exist
        assertEquals(4, BackendType.values().length);
        assertNotNull(BackendType.MONGODB);
        assertNotNull(BackendType.COSMOSDB);
        assertNotNull(BackendType.POPPY_DB);
        assertNotNull(BackendType.IN_MEMORY);
    }

    @Test
    public void testHelloResultCosmosDBField() {
        HelloResult hello = new HelloResult();
        assertNull(hello.getCosmosDB());

        hello.setCosmosDB(true);
        assertEquals(true, hello.getCosmosDB());

        hello.setCosmosDB(false);
        assertEquals(false, hello.getCosmosDB());
    }

    @Test
    public void testMorphiumConvenienceMethod() {
        // getBackendType() on Morphium delegates to driver
        BackendType type = morphium.getBackendType();
        assertNotNull(type);
        assertEquals(morphium.getDriver().getBackendType(), type);
    }

    @Test
    public void testPooledDriverPoppyDBDetection() throws Exception {
        PooledDriver driver = new PooledDriver();

        // Before detection: default is MONGODB
        assertEquals(BackendType.MONGODB, driver.getBackendType());
        assertFalse(driver.isPoppyDB());

        // Simulate hello response with poppyDB=true
        Field poppyDBField = PooledDriver.class.getDeclaredField("poppyDB");
        poppyDBField.setAccessible(true);
        poppyDBField.set(driver, true);

        assertTrue(driver.isPoppyDB());
        assertEquals(BackendType.POPPY_DB, driver.getBackendType());
    }

    @Test
    public void testPooledDriverInMemoryDetection() throws Exception {
        PooledDriver driver = new PooledDriver();

        // Simulate hello response with inMemoryBackend=true and poppyDB=true
        Field inMemoryField = PooledDriver.class.getDeclaredField("inMemoryBackend");
        inMemoryField.setAccessible(true);
        inMemoryField.set(driver, true);

        Field poppyDBField = PooledDriver.class.getDeclaredField("poppyDB");
        poppyDBField.setAccessible(true);
        poppyDBField.set(driver, true);

        assertTrue(driver.isInMemoryBackend());
        assertTrue(driver.isPoppyDB());
        // IN_MEMORY takes priority over POPPY_DB
        assertEquals(BackendType.IN_MEMORY, driver.getBackendType());
    }

    @Test
    public void testPooledDriverPlainMongoDBDetection() throws Exception {
        PooledDriver driver = new PooledDriver();
        driver.setHostSeed("mongo1:27017");

        assertFalse(driver.isCosmosDB());
        assertFalse(driver.isPoppyDB());
        assertFalse(driver.isInMemoryBackend());
        assertEquals(BackendType.MONGODB, driver.getBackendType());
    }

    @Test
    public void testBackendTypePriority() throws Exception {
        // CosmosDB + PoppyDB flags both set: CosmosDB should win over POPPY_DB
        PooledDriver driver = new PooledDriver();

        Field cosmosDBField = PooledDriver.class.getDeclaredField("cosmosDB");
        cosmosDBField.setAccessible(true);
        cosmosDBField.set(driver, true);

        Field poppyDBField = PooledDriver.class.getDeclaredField("poppyDB");
        poppyDBField.setAccessible(true);
        poppyDBField.set(driver, true);

        // IN_MEMORY > CosmosDB > POPPY_DB > MONGODB
        assertEquals(BackendType.COSMOSDB, driver.getBackendType());
    }
}
