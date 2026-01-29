# Quick Start Tutorial

*From zero to your first query in 10 minutes*

---

## Prerequisites

- Java 21+
- Maven
- Optional: MongoDB (we'll start without it!)

---

## Step 1: Add Dependency

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>morphium</artifactId>
    <version>6.1.1</version>
</dependency>
```

---

## Step 2: First Entity

Create a simple `User` class:

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
    
    // For type-safe queries (optional but recommended)
    public enum Fields { id, username, email, loginCount }
    
    // Default constructor (important for Morphium!)
    public User() {}
    
    public User(String username, String email) {
        this.username = username;
        this.email = email;
        this.loginCount = 0;
    }
    
    // Getters & Setters
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

**What's happening here?**
- `@Entity` — Marks the class as a MongoDB document
- `@Id` — This field becomes `_id` in MongoDB
- `@Index` — Automatically creates an index
- `MorphiumId` — Morphium's ObjectId wrapper

---

## Step 3: Initialize Morphium (without MongoDB!)

For starters, we'll use the **InMemory Driver** — no MongoDB required:

```java
package com.example;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

public class MorphiumDemo {
    
    public static void main(String[] args) {
        // Configuration for InMemory (no MongoDB needed!)
        MorphiumConfig cfg = new MorphiumConfig();
        cfg.setDatabase("demo");
        cfg.setDriverName(InMemoryDriver.class.getName());
        
        // Start Morphium
        Morphium morphium = new Morphium(cfg);
        System.out.println("Morphium started (InMemory mode)");
        
        // ... your code here
        
        // Cleanup
        morphium.close();
    }
}
```

**Tip:** The InMemory Driver is perfect for learning and testing. You can switch to real MongoDB later with just a config change.

---

## Step 4: CRUD Operations

### Create
```java
User alice = new User("alice", "alice@example.com");
morphium.store(alice);
System.out.println("Stored: " + alice);
// ID is automatically generated!
```

### Read
```java
// Single object
User found = morphium.createQueryFor(User.class)
    .f(User.Fields.username).eq("alice")
    .get();
System.out.println("Found: " + found);

// All users
List<User> allUsers = morphium.createQueryFor(User.class).asList();

// With conditions
List<User> activeUsers = morphium.createQueryFor(User.class)
    .f(User.Fields.loginCount).gte(5)
    .sort("-loginCount")  // Descending
    .limit(10)
    .asList();
```

### Update
```java
// Option 1: Modify object and store
found.setEmail("alice.new@example.com");
morphium.store(found);

// Option 2: Direct update (more efficient for single fields)
morphium.inc(found, User.Fields.loginCount.name(), 1);
```

### Delete
```java
morphium.delete(found);

// Or delete multiple at once
morphium.createQueryFor(User.class)
    .f(User.Fields.loginCount).eq(0)
    .delete();
```

---

## Step 5: Complete Example

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
        
        // Create: Add multiple users
        morphium.store(new User("alice", "alice@example.com"));
        morphium.store(new User("bob", "bob@example.com"));
        morphium.store(new User("charlie", "charlie@example.com"));
        System.out.println("Created 3 users");
        
        // Read: Find user
        User alice = morphium.createQueryFor(User.class)
            .f(User.Fields.username).eq("alice")
            .get();
        System.out.println("Found: " + alice);
        
        // Update: Increment login count
        morphium.inc(alice, "loginCount", 1);
        System.out.println("Incremented login count");
        
        // Read: All users with loginCount > 0
        List<User> activeUsers = morphium.createQueryFor(User.class)
            .f(User.Fields.loginCount).gt(0)
            .asList();
        System.out.println("Active users: " + activeUsers.size());
        
        // Count
        long totalUsers = morphium.createQueryFor(User.class).countAll();
        System.out.println("Total: " + totalUsers + " users");
        
        // Delete
        morphium.delete(alice);
        System.out.println("Deleted alice");
        
        // Cleanup
        morphium.close();
        System.out.println("Done!");
    }
}
```

---

## Step 6: Switch to Real MongoDB

When you're ready to use real MongoDB:

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDatabase("myapp");
cfg.addHostToSeed("localhost:27017");
// Optional: Auth
// cfg.setCredentials("user", "password", "admin");

Morphium morphium = new Morphium(cfg);
```

**That's it.** Your code stays identical — only the config changes.

---

## Common Mistakes to Avoid

### 1. Missing Default Constructor
```java
// ❌ Wrong
public class User {
    public User(String name) { ... }
}

// ✅ Correct
public class User {
    public User() {}  // Default constructor!
    public User(String name) { ... }
}
```

### 2. Field Names as Strings
```java
// ❌ Error-prone
.f("usernmae").eq("alice")  // Typo!

// ✅ Type-safe
.f(User.Fields.username).eq("alice")
```

### 3. Not Closing Morphium
```java
// ❌ Resource leak
Morphium m = new Morphium(cfg);
// ... forgot to close

// ✅ Correct
try (Morphium m = new Morphium(cfg)) {
    // ... work
}  // automatically closed
```

---

## Next Steps

You can now:
- ✅ Define entities
- ✅ Perform CRUD operations
- ✅ Write queries with conditions

**Continue with:**
- [Write Your First Test](./first-test.md)
- [Annotations in Detail](./developer-guide.md#annotations)
- [Using Messaging](./messaging.md)

---

*Questions? Problems? → [Troubleshooting Guide](./troubleshooting-guide.md)*
