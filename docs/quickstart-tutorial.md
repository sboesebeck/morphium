# Quick Start Tutorial

*Von Null zur ersten Query in 10 Minuten*

---

## Voraussetzungen

- Java 21+
- Maven
- Optional: MongoDB (wir starten ohne!)

---

## Schritt 1: Dependency hinzufügen

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>morphium</artifactId>
    <version>6.1.1</version>
</dependency>
```

---

## Schritt 2: Erstes Entity

Erstelle eine simple `User`-Klasse:

```java
package com.example;

import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;

@Entity(collectionName = "users")
public class User {
    
    @Id
    private MorphiumId id;
    
    @Index
    private String username;
    
    private String email;
    
    private int loginCount;
    
    // Für typsichere Queries (optional, aber empfohlen)
    public enum Fields { id, username, email, loginCount }
    
    // Default Constructor (wichtig für Morphium!)
    public User() {}
    
    public User(String username, String email) {
        this.username = username;
        this.email = email;
        this.loginCount = 0;
    }
    
    // Getter & Setter
    public MorphiumId getId() { return id; }
    public void setId(MorphiumId id) { this.id = id; }
    
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public int getLoginCount() { return loginCount; }
    public void setLoginCount(int loginCount) { this.loginCount = loginCount; }
    
    @Override
    public String toString() {
        return "User{id=" + id + ", username='" + username + "', email='" + email + "'}";
    }
}
```

**Was passiert hier?**
- `@Entity` — Markiert die Klasse als MongoDB-Dokument
- `@Id` — Das Feld wird zur `_id` in MongoDB
- `@Index` — Erstellt automatisch einen Index
- `MorphiumId` — Morphiums ObjectId-Wrapper

---

## Schritt 3: Morphium initialisieren (ohne MongoDB!)

Für den Anfang nutzen wir den **InMemory Driver** — kein MongoDB nötig:

```java
package com.example;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

public class MorphiumDemo {
    
    public static void main(String[] args) {
        // Konfiguration für InMemory (kein MongoDB nötig!)
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("demo");
        cfg.setDriverName(InMemoryDriver.class.getName());
        
        // Morphium starten
        Morphium morphium = new Morphium(cfg);
        System.out.println("Morphium gestartet (InMemory Mode)");
        
        // ... hier kommt dein Code
        
        // Aufräumen
        morphium.close();
    }
}
```

**Tipp:** Der InMemory Driver ist perfekt zum Lernen und für Tests. Später wechselst du einfach auf echtes MongoDB.

---

## Schritt 4: CRUD Operationen

### Create
```java
User alice = new User("alice", "alice@example.com");
morphium.store(alice);
System.out.println("Gespeichert: " + alice);
// ID wird automatisch generiert!
```

### Read
```java
// Einzelnes Objekt
User found = morphium.createQueryFor(User.class)
    .f(User.Fields.username).eq("alice")
    .get();
System.out.println("Gefunden: " + found);

// Alle User
List<User> allUsers = morphium.createQueryFor(User.class).asList();

// Mit Bedingungen
List<User> activeUsers = morphium.createQueryFor(User.class)
    .f(User.Fields.loginCount).gte(5)
    .sort("-loginCount")  // Absteigend
    .limit(10)
    .asList();
```

### Update
```java
// Variante 1: Objekt ändern und speichern
found.setEmail("alice.new@example.com");
morphium.store(found);

// Variante 2: Direktes Update (effizienter für einzelne Felder)
morphium.inc(found, User.Fields.loginCount.name(), 1);
```

### Delete
```java
morphium.delete(found);

// Oder mehrere auf einmal
morphium.createQueryFor(User.class)
    .f(User.Fields.loginCount).eq(0)
    .delete();
```

---

## Schritt 5: Komplettes Beispiel

```java
package com.example;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import java.util.List;

public class MorphiumDemo {
    
    public static void main(String[] args) {
        // Setup
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("demo");
        cfg.setDriverName(InMemoryDriver.class.getName());
        Morphium morphium = new Morphium(cfg);
        
        // Create: Mehrere User anlegen
        morphium.store(new User("alice", "alice@example.com"));
        morphium.store(new User("bob", "bob@example.com"));
        morphium.store(new User("charlie", "charlie@example.com"));
        System.out.println("3 User angelegt");
        
        // Read: User finden
        User alice = morphium.createQueryFor(User.class)
            .f(User.Fields.username).eq("alice")
            .get();
        System.out.println("Gefunden: " + alice);
        
        // Update: Login-Count erhöhen
        morphium.inc(alice, "loginCount", 1);
        System.out.println("Login-Count erhöht");
        
        // Read: Alle User mit loginCount > 0
        List<User> activeUsers = morphium.createQueryFor(User.class)
            .f(User.Fields.loginCount).gt(0)
            .asList();
        System.out.println("Aktive User: " + activeUsers.size());
        
        // Count
        long totalUsers = morphium.createQueryFor(User.class).countAll();
        System.out.println("Gesamt: " + totalUsers + " User");
        
        // Delete
        morphium.delete(alice);
        System.out.println("Alice gelöscht");
        
        // Cleanup
        morphium.close();
        System.out.println("Done!");
    }
}
```

---

## Schritt 6: Wechsel zu echtem MongoDB

Wenn du bereit bist, echtes MongoDB zu nutzen:

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDatabase("myapp");
cfg.addHostToSeed("localhost:27017");
// Optional: Auth
// cfg.setCredentials("user", "password", "admin");

Morphium morphium = new Morphium(cfg);
```

**Das war's.** Dein Code bleibt identisch — nur die Config ändert sich.

---

## Typische Fehler vermeiden

### 1. Kein Default Constructor
```java
// ❌ Falsch
public class User {
    public User(String name) { ... }
}

// ✅ Richtig
public class User {
    public User() {}  // Default Constructor!
    public User(String name) { ... }
}
```

### 2. Feldnamen als Strings
```java
// ❌ Fehleranfällig
.f("usernmae").eq("alice")  // Typo!

// ✅ Typsicher
.f(User.Fields.username).eq("alice")
```

### 3. Morphium nicht schließen
```java
// ❌ Resource Leak
Morphium m = new Morphium(cfg);
// ... vergessen zu schließen

// ✅ Richtig
try (Morphium m = new Morphium(cfg)) {
    // ... arbeiten
}  // automatisch geschlossen
```

---

## Nächste Schritte

Du kannst jetzt:
- ✅ Entities definieren
- ✅ CRUD Operationen ausführen
- ✅ Queries mit Bedingungen schreiben

**Weiter geht's mit:**
- [Deinen ersten Test schreiben](./first-test.md)
- [Annotations im Detail](./developer-guide.md#annotations)
- [Messaging nutzen](./messaging.md)

---

*Fragen? Probleme? → [Troubleshooting Guide](./troubleshooting-guide.md)*
