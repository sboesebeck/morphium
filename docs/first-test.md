# Deinen ersten Test schreiben

*Unit Tests mit Morphium — schnell, isoliert, ohne Docker*

---

## Warum der InMemory Driver für Tests?

| Ansatz | Startup | Isolation | CI/CD |
|--------|---------|-----------|-------|
| Testcontainers | ~5-10s | ✅ Gut | Braucht Docker |
| Shared Test-DB | Sofort | ❌ Probleme | Race Conditions |
| Mocks | Sofort | ✅ Gut | Aufwändig zu schreiben |
| **InMemory Driver** | **~50ms** | **✅ Perfekt** | **Kein Docker nötig** |

Der InMemory Driver emuliert MongoDB komplett im RAM — inklusive Aggregation Pipelines, Change Streams und Transactions.

---

## Basis-Setup (JUnit 5)

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

### Einfacher Test

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
        cfg.setDatabase("test_" + System.currentTimeMillis()); // Unique DB pro Test
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
        assertNotNull(user.getId(), "ID sollte generiert werden");
        
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

## Test-Basisklasse (wiederverwendbar)

Für größere Projekte: Eine Basisklasse, die Setup/Teardown kapselt.

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
     * Hilfsmethode: Warten bis Async-Operationen fertig sind
     */
    protected void waitForAsyncWrites() {
        while (morphium.getWriteBufferCount() > 0) {
            Thread.onSpinWait();
        }
    }
}
```

**Verwendung:**
```java
class OrderServiceTest extends MorphiumTestBase {
    
    @Test
    void shouldCreateOrder() {
        // morphium ist bereits initialisiert
        Order order = new Order();
        morphium.store(order);
        // ...
    }
}
```

---

## Tests für Messaging

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
        
        // Then: Warten auf Message-Verarbeitung
        Thread.sleep(500);
        assertEquals("payload", received.get());
        
        // Cleanup
        sender.terminate();
        receiver.terminate();
    }
}
```

---

## Tests gegen echtes MongoDB (optional)

Manchmal willst du gegen echtes MongoDB testen (Integration Tests):

```java
@Tag("integration")  // Separat ausführbar
class MongoDBIntegrationTest {
    
    private Morphium morphium;
    
    @BeforeEach
    void setUp() {
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("integration_test");
        cfg.addHostToSeed("localhost:27017");
        morphium = new Morphium(cfg);
        
        // Alte Daten löschen
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
        // Test gegen echtes MongoDB
    }
}
```

**Ausführung:**
```bash
# Nur Unit Tests (InMemory)
mvn test

# Nur Integration Tests
mvn test -Dgroups=integration

# Alles
mvn test -Dgroups="integration,!slow"
```

---

## Morphiums Test-Runner: `runtests.sh`

Morphium selbst hat einen mächtigen Test-Runner:

```bash
# Alle Tests mit InMemory Driver (Standard)
./runtests.sh

# Mit echtem MongoDB
./runtests.sh --driver pooled --uri "mongodb://localhost:27017/test"

# Nur bestimmte Tags
./runtests.sh --tags core
./runtests.sh --tags messaging

# Parallel
./runtests.sh --parallel 4

# Einzelne Klasse
./runtests.sh --test-class de.caluga.test.morphium.BasicFunctionalityTests

# Hilfe
./runtests.sh --help
```

---

## Best Practices

### 1. Jeder Test bekommt eigene DB
```java
cfg.setDatabase("test_" + System.currentTimeMillis());
```
Verhindert Test-Interferenzen.

### 2. Nutze Fields-Enum
```java
// ✅ Refactoring-sicher
.f(User.Fields.username).eq("test")

// ❌ Bricht bei Rename
.f("username").eq("test")
```

### 3. Async-Writes abwarten
```java
morphium.store(obj);
// Bei async writes:
while (morphium.getWriteBufferCount() > 0) {
    Thread.onSpinWait();
}
```

### 4. Cleanup nicht vergessen
```java
@AfterEach
void cleanup() {
    morphium.close();  // Wichtig!
}
```

### 5. InMemory für Unit Tests, echtes MongoDB für Integration
```java
@Tag("unit")
class FastUnitTest extends MorphiumTestBase { }

@Tag("integration") 
class SlowIntegrationTest { /* echtes MongoDB */ }
```

---

## Fehlerbehebung

### Test hängt
Wahrscheinlich Messaging-Listener nicht terminiert:
```java
messaging.terminate();
```

### "Collection not found"
InMemory Driver erstellt Collections lazy. Erst nach erstem `store()` existiert sie.

### Flaky Tests
- Prüfe auf Race Conditions bei Messaging
- Nutze `Thread.sleep()` oder besser: CountDownLatch
- Unique DB-Namen pro Test verwenden

---

## Nächste Schritte

- [Developer Testing Guide](./developer-testing-guide.md) — Tiefer in Morphiums eigene Test-Infrastruktur
- [Test Runner Reference](./test-runner.md) — Alle `runtests.sh` Optionen
- [Messaging Guide](./messaging.md) — Messaging testen

---

*Fragen? → [Troubleshooting Guide](./troubleshooting-guide.md)*
