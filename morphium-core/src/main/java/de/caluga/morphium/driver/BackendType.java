package de.caluga.morphium.driver;

/**
 * Identifies the type of database backend Morphium is connected to.
 * Detected automatically at connect time via the hello handshake.
 */
public enum BackendType {
    MONGODB,           // Standard MongoDB
    COSMOSDB,          // Azure CosmosDB for MongoDB API
    POPPY_DB,          // PoppyDB (Netty-based)
    IN_MEMORY          // InMemoryDriver (Tests)
}
