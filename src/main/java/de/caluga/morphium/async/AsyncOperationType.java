package de.caluga.morphium.async;

/**
 * User: Stephan Bösebeck
 * Date: 13.02.13
 * Time: 15:41
 * <p>
 * Different types of asynchronous operations. needed for callbacks and used inernally
 */
public enum AsyncOperationType {
    READ, WRITE, UPDATE, SET, INC, UNSET, PUSH, PULL, REMOVE, ENSURE_INDICES, BULK,
}
