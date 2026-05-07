package de.caluga.morphium.driver;

/**
 * Thrown when a write is rejected by MongoDB because a single document exceeds the
 * 16 MB BSON limit. Mongo signals this with error code 10334
 * ("BSONObj size: N is invalid. Size must be between 0 and 16793600(16MB)") or, on
 * some versions, with code 17280 ("BSON object too large for storage").
 *
 * The hard limit is enforced by the server and cannot be raised. Typical fixes:
 *   - split the payload into multiple documents
 *   - store large blobs in GridFS or external storage and persist only a reference
 *   - use streaming for large data transfers
 *
 * For messaging specifically: avoid sending oversized answer documents — split them
 * into multiple messages with the same {@code inAnswerTo} or use an external transport
 * for large payloads.
 */
public class MorphiumDocumentTooLargeException extends MorphiumDriverException {
    public MorphiumDocumentTooLargeException(String message) {
        super(message);
    }

    public MorphiumDocumentTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }
}
