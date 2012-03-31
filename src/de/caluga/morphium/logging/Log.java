/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.logging;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.Cache;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @author stephan
 */
@Cache(clearOnWrite = false, maxEntries = 0, overridable = false, readCache = false, writeCache = true)
@Entity
public class Log {
    @Id
    private ObjectId id;
    private String level;
    private long sequence;
    private String message;
    private List<String> params;
    private String exceptionName;
    private String exceptionMessage;
    private List<String> exceptionStacktrace;
    private String causedBy;
    private String sourceClass;
    private String sourceMethod;
    private String threadName;
    private int threadId;
    private long timestamp;

    public ObjectId getId() {
        return id;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getCausedBy() {
        return causedBy;
    }

    public void setCausedBy(String causedBy) {
        this.causedBy = causedBy;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }

    public String getExceptionName() {
        return exceptionName;
    }

    public void setExceptionName(String exceptionName) {
        this.exceptionName = exceptionName;
    }

    public List<String> getExceptionStacktrace() {
        return exceptionStacktrace;
    }

    public void setExceptionStacktrace(List<String> exceptionStacktrace) {
        this.exceptionStacktrace = exceptionStacktrace;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<String> getParams() {
        return params;
    }

    public void setParams(List<String> params) {
        this.params = params;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public String getSourceClass() {
        return sourceClass;
    }

    public void setSourceClass(String sourceClass) {
        this.sourceClass = sourceClass;
    }

    public String getSourceMethod() {
        return sourceMethod;
    }

    public void setSourceMethod(String sourceMethod) {
        this.sourceMethod = sourceMethod;
    }

    public int getThreadId() {
        return threadId;
    }

    public void setThreadId(int threadId) {
        this.threadId = threadId;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


}
