# Security Guide

Security considerations and best practices for Morphium applications in MongoDB Community Edition environments.

## Security Model Overview

**Morphium is built primarily for MongoDB Community Edition**, which has specific security limitations compared to MongoDB Enterprise. Understanding these constraints is essential for proper security planning.

### Community Edition Limitations

**No SSL/TLS Support:**
- MongoDB Community Edition **does not support SSL/TLS encryption**
- Morphium's wire protocol driver **does not implement SSL/TLS**
- All network communication is **unencrypted**

**Basic Authentication Only:**
- Only **username/password authentication** supported
- No support for:
  - LDAP authentication
  - Kerberos authentication  
  - x.509 certificate authentication
  - SCRAM-SHA-256 (depends on MongoDB version)

**Limited Authorization:**
- Basic role-based access control (RBAC)
- No advanced security features like:
  - Auditing
  - Field-level security
  - Encryption at rest (without additional tools)

## Network Security

### Trusted Network Requirement

**Since SSL/TLS is not available, Morphium must be deployed in trusted network environments:**

```java
// Configure for trusted network deployment
MorphiumConfig cfg = new MorphiumConfig();

// Internal network hosts only
cfg.clusterSettings().addHostToSeed("internal-mongo1.company.local", 27017);
cfg.clusterSettings().addHostToSeed("internal-mongo2.company.local", 27017);
cfg.clusterSettings().addHostToSeed("internal-mongo3.company.local", 27017);

// Use internal DNS names, not public IPs
```

### Network Security Best Practices

**1. Network Isolation:**
```bash
# MongoDB should only be accessible from application networks
# Use firewall rules to restrict access

# Allow only application servers
iptables -A INPUT -p tcp --dport 27017 -s 10.0.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 27017 -j DROP

# Or use cloud security groups
# AWS: Restrict to application security groups only  
# Azure: Use NSG rules for internal access only
# GCP: Configure VPC firewall rules
```

**2. VPN/Private Networks:**
```bash
# Deploy MongoDB and applications in:
# - Private VPC/VNet subnets
# - Corporate VPN networks  
# - Dedicated private networks
# - Container overlay networks (Docker, Kubernetes)
```

**3. Network Monitoring:**
```java
// Monitor for unusual connection attempts
@Component
public class SecurityMonitor {
    
    @EventListener
    public void onConnectionError(ConnectionErrorEvent event) {
        // Log failed connection attempts for security analysis
        securityLogger.warn("Connection attempt failed from: {} to: {}", 
                           event.getSourceIP(), event.getTargetHost());
    }
}
```

## Authentication and Authorization

### Basic Authentication Configuration

**Application Authentication:**
```java
MorphiumConfig cfg = new MorphiumConfig();

// Use environment variables for credentials
cfg.authSettings().setMongoLogin(System.getenv("MONGO_USERNAME"));
cfg.authSettings().setMongoPassword(System.getenv("MONGO_PASSWORD"));

// For replica set operations (if needed)
cfg.authSettings().setMongoAdminUser(System.getenv("MONGO_ADMIN_USER"));
cfg.authSettings().setMongoAdminPwd(System.getenv("MONGO_ADMIN_PWD"));
```

**Never hardcode credentials:**
```java
// ❌ NEVER DO THIS
cfg.authSettings().setMongoLogin("hardcoded_user");
cfg.authSettings().setMongoPassword("hardcoded_password");

// ✅ USE ENVIRONMENT VARIABLES OR SECURE VAULTS
cfg.authSettings().setMongoLogin(System.getenv("MONGO_USERNAME"));
cfg.authSettings().setMongoPassword(getFromSecureVault("mongo.password"));
```

### MongoDB User Management

**Create Application-Specific Users:**
```javascript
// In MongoDB shell - create dedicated application user
use myapp_prod

// Application user with minimal required permissions
db.createUser({
  user: "myapp_user",
  pwd: "secure_random_password",
  roles: [
    { role: "readWrite", db: "myapp_prod" },
    { role: "read", db: "myapp_config" }
  ]
})

// Admin user for replica set operations (if needed)
use admin
db.createUser({
  user: "myapp_admin",
  pwd: "admin_secure_password", 
  roles: [
    { role: "read", db: "local" },
    { role: "clusterMonitor", db: "admin" }
  ]
})
```

**Role-Based Access Control:**
```javascript
// Custom role for specific application needs
use admin
db.createRole({
  role: "myAppRole",
  privileges: [
    {
      resource: { db: "myapp_prod", collection: "" },
      actions: ["find", "insert", "update", "remove"]
    },
    {
      resource: { db: "myapp_prod", collection: "sensitive_collection" },
      actions: ["find"] // Read-only for sensitive data
    }
  ],
  roles: []
})

// Assign custom role to user
use myapp_prod
db.grantRolesToUser("myapp_user", ["myAppRole"])
```

## Data Protection

### Application-Level Encryption

**Since MongoDB Community Edition lacks encryption at rest, implement application-level encryption:**

```java
// Field-level encryption using Morphium's encryption support
@Entity
public class SensitiveData {
    @Id
    private MorphiumId id;
    
    @Encrypted // Encrypted before storing to MongoDB
    private String socialSecurityNumber;
    
    @Encrypted
    private String creditCardNumber;
    
    private String publicData; // Not encrypted
}

// Configure encryption provider
cfg.encryptionSettings().setEncryptionKeyProvider(new AESEncryptionProvider());
cfg.encryptionSettings().setEncryptionKey(getEncryptionKey());
```

**Custom Encryption Implementation:**
```java
@Component
public class DataEncryption {
    
    private final AESUtil aes;
    
    public String encryptSensitiveField(String plainText) {
        if (plainText == null) return null;
        return aes.encrypt(plainText, getFieldEncryptionKey());
    }
    
    public String decryptSensitiveField(String encryptedText) {
        if (encryptedText == null) return null;
        return aes.decrypt(encryptedText, getFieldEncryptionKey());
    }
    
    private String getFieldEncryptionKey() {
        // Get from secure key management system
        return keyManager.getKey("field_encryption_key");
    }
}
```

### Sensitive Data Handling

**PII Data Protection:**
```java
@Entity
public class UserProfile {
    @Id
    private MorphiumId id;
    
    @Encrypted
    private String email;
    
    @Encrypted  
    private String phoneNumber;
    
    // Hash instead of storing directly
    private String passwordHash;
    
    // Tokenize sensitive IDs
    private String userToken; // Use instead of actual user ID externally
}

// Implement data masking for logs
@Override
public String toString() {
    return "UserProfile{" +
           "id=" + id +
           ", email='" + maskEmail(email) + '\'' +
           ", phoneNumber='" + maskPhone(phoneNumber) + '\'' +
           '}';
}
```

## Secure Configuration Management

### Environment-Based Configuration

**Development Environment:**
```java
// development.properties
database=myapp_dev
hosts=localhost:27017
mongo_login=dev_user
mongo_password=dev_password
log_level=5
```

**Production Environment:**
```java
// production.properties - minimal logging
database=myapp_prod
hosts=prod-mongo1:27017,prod-mongo2:27017,prod-mongo3:27017
mongo_login=${MONGO_USERNAME}
mongo_password=${MONGO_PASSWORD}
log_level=3
```

**Container Security:**
```dockerfile
# Use non-root user
RUN adduser --disabled-password --gecos '' appuser
USER appuser

# Use secrets for sensitive data
# Pass via environment variables from orchestrator
ENV MONGO_USERNAME=""
ENV MONGO_PASSWORD=""

# Don't include credentials in image
COPY --chown=appuser:appuser app.jar /app/app.jar
```

**Kubernetes Secrets:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mongo-credentials
type: Opaque
data:
  username: bXlhcHBfdXNlcg== # base64 encoded
  password: c2VjdXJlX3Bhc3N3b3Jk # base64 encoded

---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: myapp
        env:
        - name: MONGO_USERNAME
          valueFrom:
            secretKeyRef:
              name: mongo-credentials
              key: username
        - name: MONGO_PASSWORD
          valueFrom:
            secretKeyRef:
              name: mongo-credentials
              key: password
```

## Audit and Monitoring

### Security Event Logging

**Implement comprehensive security logging:**
```java
@Component
public class SecurityAuditLogger {
    
    private final Logger securityLog = LoggerFactory.getLogger("SECURITY");
    
    public void logAuthenticationAttempt(String username, boolean success, String sourceIP) {
        securityLog.info("AUTH_ATTEMPT user={} success={} source={}", 
                        username, success, sourceIP);
    }
    
    public void logDataAccess(String username, String collection, String operation) {
        securityLog.info("DATA_ACCESS user={} collection={} operation={}", 
                        username, collection, operation);
    }
    
    public void logSecurityViolation(String violation, String details) {
        securityLog.warn("SECURITY_VIOLATION type={} details={}", violation, details);
    }
}
```

**Structured Security Logging:**
```java
// Use structured logging for security events
@Component  
public class StructuredSecurityLogger {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public void logSecurityEvent(SecurityEvent event) {
        try {
            String jsonEvent = objectMapper.writeValueAsString(event);
            securityLogger.info("SECURITY_EVENT: {}", jsonEvent);
        } catch (Exception e) {
            securityLogger.error("Failed to log security event", e);
        }
    }
}

public class SecurityEvent {
    private String eventType;
    private String username;  
    private String sourceIP;
    private String resource;
    private String operation;
    private boolean success;
    private LocalDateTime timestamp;
    // ... getters/setters
}
```

### Monitoring Security Metrics

**Track security-related metrics:**
```java
@Component
public class SecurityMetrics {
    
    private final MeterRegistry meterRegistry;
    
    // Count authentication failures
    private final Counter authFailures = Counter.builder("morphium.auth.failures")
                                                .register(meterRegistry);
    
    // Count data access by type
    private final Counter dataAccess = Counter.builder("morphium.data.access")
                                              .tag("operation", "read")
                                              .register(meterRegistry);
    
    public void recordAuthFailure(String reason) {
        authFailures.increment(Tags.of("reason", reason));
    }
    
    public void recordDataAccess(String operation, String collection) {
        dataAccess.increment(Tags.of("operation", operation, "collection", collection));
    }
}
```

## Input Validation and Injection Prevention

### Query Injection Prevention

**Use Morphium's type-safe query API:**
```java
// ✅ SAFE - Uses parameterized queries
Query<User> safeQuery = morphium.createQueryFor(User.class)
    .f("username").eq(userInput)  // Automatically escaped/parameterized
    .f("status").eq("active");

// ❌ DANGEROUS - Raw query construction
Query<User> unsafeQuery = morphium.createQueryFor(User.class)
    .complexQuery(Doc.of("username", userInput)); // Could allow injection
```

**Input validation:**
```java
@Component
public class InputValidator {
    
    public void validateUsername(String username) {
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Username cannot be empty");
        }
        
        if (username.length() > 50) {
            throw new IllegalArgumentException("Username too long");
        }
        
        if (!username.matches("^[a-zA-Z0-9_-]+$")) {
            throw new IllegalArgumentException("Username contains invalid characters");
        }
    }
    
    public void validateEmail(String email) {
        if (email == null || !email.matches("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$")) {
            throw new IllegalArgumentException("Invalid email format");
        }
    }
}
```

## Secure Development Practices

### Code Security Review Checklist

**Configuration Security:**
- [ ] No hardcoded credentials in source code
- [ ] Environment variables used for sensitive data
- [ ] Minimal database permissions granted
- [ ] Network access properly restricted

**Data Protection:**
- [ ] Sensitive fields encrypted at application level
- [ ] PII data properly masked in logs
- [ ] Input validation implemented
- [ ] Output encoding applied where needed

**Authentication/Authorization:**
- [ ] Strong password policies enforced
- [ ] User roles properly defined and assigned
- [ ] Session management secure
- [ ] Access controls verified

**Logging and Monitoring:**
- [ ] Security events properly logged
- [ ] Sensitive data not logged
- [ ] Log files properly secured
- [ ] Monitoring alerts configured

### Security Testing

**Automated Security Testing:**
```java
@Test
public class SecurityTests {
    
    @Test
    public void testNoCredentialsInLogs() {
        // Verify no passwords appear in log files
        String logContent = readLogFile();
        assertFalse("Password leaked in logs", 
                   logContent.contains("password"));
    }
    
    @Test 
    public void testInputValidation() {
        // Test SQL injection attempts
        assertThrows(IllegalArgumentException.class, () -> {
            userService.findUser("'; DROP TABLE users; --");
        });
    }
    
    @Test
    public void testEncryption() {
        // Verify sensitive data is encrypted
        SensitiveData data = new SensitiveData();
        data.setSocialSecurityNumber("123-45-6789");
        
        morphium.store(data);
        
        // Verify stored data is encrypted (not plaintext)
        Doc stored = morphium.getDatabase()
                            .getCollection("sensitive_data")
                            .find(Doc.of("_id", data.getId()))
                            .first();
        
        assertNotEquals("123-45-6789", stored.getString("socialSecurityNumber"));
    }
}
```

## Security Incident Response

### Incident Response Plan

**1. Immediate Response:**
- Identify compromised accounts/systems
- Revoke compromised credentials immediately
- Block suspicious network traffic
- Isolate affected systems

**2. Investigation:**
- Analyze security logs for attack patterns
- Identify data that may have been accessed
- Determine attack vector and timeline
- Document all findings

**3. Recovery:**
- Patch security vulnerabilities
- Update all credentials
- Restore from clean backups if needed
- Implement additional security measures

**4. Post-Incident:**
- Update security procedures
- Enhance monitoring and alerting  
- Conduct security training
- Review and test incident response plan

This security guide provides comprehensive coverage of security considerations for Morphium applications in MongoDB Community Edition environments, focusing on network security, authentication, data protection, and secure development practices.