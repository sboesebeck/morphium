# Writing Your First Test

*Unit tests with Morphium — fast, isolated, no Docker required*

---

## Why the InMemory Driver for Tests?

| Approach | Startup | Isolation | CI/CD |
|----------|---------|-----------|-------|
| Testcontainers | ~5-10s | ✅ Good | Requires Docker |
| Shared test DB | Instant | ❌ Problems | Race conditions |
| Mocks | Instant | ✅ Good | Tedious to write |
| **InMemory Driver** | **~50ms** | **✅ Perfect** | **No Docker needed** |

The InMemory Driver emulates MongoDB completely in RAM — including aggregation pipelines, change streams, and transactions.

---

## Basic Setup (JUnit 5)

### Dependencies

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>morphium</artifactId>
    <version>6.1.1</version>
</dependency>
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>5.10.0</version>
    <scope>test</scope>
</dependency>
```

### Simple Test

```java
package com.example;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class UserTest {
    
    private Morphium morphium;
    
    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("test_" + System.currentTimeMillis()); // Unique DB per test
        cfg.setDriverName(InMemoryDriver.class.getName());
        morphium = new Morphium(cfg);
    }
    
    @AfterEach
    void tearDown() {
        if (morphium != null) {
            morphium.close();
        }
    }
    
    @Test
    void shouldStoreAndRetrieveUser() {
        // Given
        User user = new User("testuser", "test@example.com");
        
        // When
        morphium.store(user);
        
        // Then
        assertNotNull(user.getId(), "ID should be generated");
        
        User found = morphium.createQueryFor(User.class)
            .f(User.Fields.username).eq("testuser")
            .get();
            
        assertNotNull(found);
        assertEquals("testuser", found.getUsername());
        assertEquals("test@example.com", found.getEmail());
    }
    
    @Test
    void shouldUpdateUser() {
        // Given
        User user = new User("updateme", "old@example.com");
        morphium.store(user);
        
        // When
        user.setEmail("new@example.com");
        morphium.store(user);
        
        // Then
        User found = morphium.findById(User.class, user.getId());
        assertEquals("new@example.com", found.getEmail());
    }
    
    @Test
    void shouldDeleteUser() {
        // Given
        User user = new User("deleteme", "bye@example.com");
        morphium.store(user);
        
        // When
        morphium.delete(user);
        
        // Then
        User found = morphium.findById(User.class, user.getId());
        assertNull(found);
    }
    
    @Test
    void shouldQueryWithConditions() {
        // Given
        morphium.store(new User("alice", "alice@example.com"));
        morphium.store(new User("bob", "bob@example.com"));
        morphium.store(new User("charlie", "charlie@example.com"));
        
        // When
        long count = morphium.createQueryFor(User.class)
            .f(User.Fields.username).in(java.util.List.of("alice", "bob"))
            .countAll();
        
        // Then
        assertEquals(2, count);
    }
}
```

---

## Test Base Class (reusable)

For larger projects: A base class that encapsulates setup/teardown.

```java
package com.example.test;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class MorphiumTestBase {
    
    protected Morphium morphium;
    
    @BeforeEach
    void setUpMorphium() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("test_" + getClass().getSimpleName() + "_" + System.currentTimeMillis());
        cfg.setDriverName(InMemoryDriver.class.getName());
        morphium = new Morphium(cfg);
    }
    
    @AfterEach
    void tearDownMorphium() {
        if (morphium != null) {
            morphium.close();
        }
    }
    
    /**
     * Helper method: Wait until async operations are complete
     */
    protected void waitForAsyncWrites() {
        while (morphium.getWriteBufferCount() > 0) {
            Thread.onSpinWait();
        }
    }
}
```

**Usage:**
```java
class OrderServiceTest extends MorphiumTestBase {
    
    @Test
    void shouldCreateOrder() {
        // morphium is already initialized
        Order order = new Order();
        morphium.store(order);
        // ...
    }
}
```

---

## Testing Messaging

```java
import de.caluga.morphium.messaging.Messaging;
import de.caluga.morphium.messaging.Msg;
import java.util.concurrent.atomic.AtomicReference;

class MessagingTest extends MorphiumTestBase {
    
    @Test
    void shouldSendAndReceiveMessage() throws InterruptedException {
        // Given
        Messaging sender = new Messaging(morphium, 100, true);
        Messaging receiver = new Messaging(morphium, 100, true);
        
        AtomicReference<String> received = new AtomicReference<>();
        
        receiver.addMessageListener((messaging, msg) -> {
            received.set(msg.getValue());
            return null;
        });
        
        // When
        sender.sendMessage(new Msg("test-topic", "Hello World", "payload"));
        
        // Then: Wait for message processing
        Thread.sleep(500);
        assertEquals("payload", received.get());
        
        // Cleanup
        sender.terminate();
        receiver.terminate();
    }
}
```

---

## Testing Against Real MongoDB (optional)

Sometimes you want to test against real MongoDB (integration tests):

```java
@Tag("integration")  // Run separately
class MongoDBIntegrationTest {
    
    private Morphium morphium;
    
    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("integration_test");
        cfg.addHostToSeed("localhost:27017");
        morphium = new Morphium(cfg);
        
        // Clear old data
        morphium.clearCollection(User.class);
    }
    
    @AfterEach
    void tearDown() {
        if (morphium != null) {
            morphium.clearCollection(User.class);
            morphium.close();
        }
    }
    
    @Test
    void shouldWorkWithRealMongoDB() {
        // Test against real MongoDB
    }
}
```

**Running tests:**
```bash
# Unit tests only (InMemory)
mvn test

# Integration tests only
mvn test -Dgroups=integration

# Everything
mvn test -Dgroups="integration,!slow"
```

---

## Morphium's Test Runner: `runtests.sh`

Morphium itself has a powerful test runner:

```bash
# All tests with InMemory Driver (default)
./runtests.sh

# With real MongoDB
./runtests.sh --driver pooled --uri "mongodb://localhost:27017/test"

# Only specific tags
./runtests.sh --tags core
./runtests.sh --tags messaging

# Parallel
./runtests.sh --parallel 4

# Single class
./runtests.sh --test de.caluga.test.morphium.BasicFunctionalityTests

# Help
./runtests.sh --help
```

---

## Best Practices

### 1. Each Test Gets Its Own DB
```java
cfg.setDatabase("test_" + System.currentTimeMillis());
```
Prevents test interference.

### 2. Use the Fields Enum
```java
// ✅ Refactoring-safe
.f(User.Fields.username).eq("test")

// ❌ Breaks on rename
.f("username").eq("test")
```

### 3. Wait for Async Writes
```java
morphium.store(obj);
// For async writes:
while (morphium.getWriteBufferCount() > 0) {
    Thread.onSpinWait();
}
```

### 4. Don't Forget Cleanup
```java
@AfterEach
void cleanup() {
    morphium.close();  // Important!
}
```

### 5. InMemory for Unit Tests, Real MongoDB for Integration
```java
@Tag("unit")
class FastUnitTest extends MorphiumTestBase { }

@Tag("integration") 
class SlowIntegrationTest { /* real MongoDB */ }
```

---

## Troubleshooting

### Test Hangs
Probably a messaging listener not terminated:
```java
messaging.terminate();
```

### "Collection not found"
InMemory Driver creates collections lazily. They only exist after the first `store()`.

### Flaky Tests
- Check for race conditions in messaging
- Use `Thread.sleep()` or better: CountDownLatch
- Use unique DB names per test

---

## Next Steps

- [Developer Testing Guide](./developer-testing-guide.md) — Deep dive into Morphium's own test infrastructure
- [Test Runner Reference](./test-runner.md) — All `runtests.sh` options
- [Messaging Guide](./messaging.md) — Testing messaging

---

*Questions? → [Troubleshooting Guide](./troubleshooting-guide.md)*
