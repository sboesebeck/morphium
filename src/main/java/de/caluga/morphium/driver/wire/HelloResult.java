package de.caluga.morphium.driver.wire;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.slf4j.LoggerFactory;

public class HelloResult {
    private static AnnotationAndReflectionHelper an = new AnnotationAndReflectionHelper(false);
    private Boolean helloOk;
    private Boolean isWritablePrimary;
    private Integer maxBsonObjectSize = 16 * 1024 * 1024;
    private Integer maxMessageSizeBytes = 48000000;
    private Integer maxWriteBatchSize = 100000;
    private Date localTime;
    private Integer logicalSessionTimeoutMinutes;
    private Integer connectionId;
    private Integer minWireVersion;
    private Integer maxWireVersion;
    private Boolean readOnly = false;
    private String compression;
    private List<String> saslSupportedMechs;
    //Contains the value isdbgrid when hello returns from a mongos instance.
    private String msg;
    //Replicaset only
    private List<String> hosts;
    private String me;
    private String primary;
    private String setName;
    private Integer setVersion;
    private Boolean arbiterOnly;
    private Boolean secondary;
    private Boolean passive;
    private Boolean hidden;
    private Map<String, String> tags;
    private MorphiumId electionId;
    private Map<String, Object> lastWrite;
    private Double ok;

    private Map<String, Object> topologyVersion;
    private Long operationTime;

    // MorphiumServer-specific fields
    private Boolean morphiumServer;
    private Boolean inMemoryBackend;

    public static HelloResult fromMsg(Map<String, Object> msg) {
        if (msg == null) return null;

        var fields = an.getAllFields(HelloResult.class);
        var ret = new HelloResult();

        for (Field f : fields) {
            try {
                if (msg.containsKey(f.getName())) {
                    f.setAccessible(true);
                    f.set(ret, msg.get(f.getName()));
                }
            } catch (Exception e) {
                //something went wrong...
                //e.printStackTrace();
                LoggerFactory.getLogger(HelloResult.class).error("Exception parsing answer", e);
            }
        }

        return ret;
    }

    public Map<String, Object> toMsg() {
        var fields = an.getAllFields(HelloResult.class);
        var ret = Doc.of();

        for (Field f : fields) {
            if (Modifier.isStatic(f.getModifiers())) continue;

            try {
                f.setAccessible(true);

                if (f.get(this) != null) {
                    ret.add(f.getName(), f.get(this));
                }
            } catch (Exception e) {
                //e.printStackTrace();
                LoggerFactory.getLogger(this.getClass()).error("Exception parsing answer", e);
            }
        }

        return ret;
    }

    public Boolean getSecondary() {
        return secondary;
    }

    public HelloResult setSecondary(Boolean secondary) {
        this.secondary = secondary;
        return this;
    }

    public Boolean getWritablePrimary() {
        return isWritablePrimary;
    }

    public HelloResult setWritablePrimary(Boolean writablePrimary) {
        isWritablePrimary = writablePrimary;
        return this;
    }

    public Integer getMaxBsonObjectSize() {
        return maxBsonObjectSize;
    }

    public HelloResult setMaxBsonObjectSize(Integer maxBsonObjectSize) {
        this.maxBsonObjectSize = maxBsonObjectSize;
        return this;
    }

    public Integer getMaxMessageSizeBytes() {
        return maxMessageSizeBytes;
    }

    public HelloResult setMaxMessageSizeBytes(Integer maxMessageSizeBytes) {
        this.maxMessageSizeBytes = maxMessageSizeBytes;
        return this;
    }

    public Integer getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    public HelloResult setMaxWriteBatchSize(Integer maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
        return this;
    }


    public Map<String, Object> getTopologyVersion() {
        return topologyVersion;
    }

    public HelloResult setTopologyVersion(Map<String, Object> topologyVersion) {
        this.topologyVersion = topologyVersion;
        return this;
    }

    public Long getOperationTime() {
        return operationTime;
    }

    public HelloResult setOperationTime(Long operationTime) {
        this.operationTime = operationTime;
        return this;
    }

    public Date getLocalTime() {
        return localTime;
    }

    public HelloResult setLocalTime(Date localTime) {
        this.localTime = localTime;
        return this;
    }

    public Integer getLogicalSessionTimeoutMinutes() {
        return logicalSessionTimeoutMinutes;
    }

    public HelloResult setLogicalSessionTimeoutMinutes(Integer logicalSessionTimeoutMinutes) {
        this.logicalSessionTimeoutMinutes = logicalSessionTimeoutMinutes;
        return this;
    }

    public Integer getConnectionId() {
        return connectionId;
    }

    public HelloResult setConnectionId(Integer connectionId) {
        this.connectionId = connectionId;
        return this;
    }

    public Integer getMinWireVersion() {
        return minWireVersion;
    }

    public HelloResult setMinWireVersion(Integer minWireVersion) {
        this.minWireVersion = minWireVersion;
        return this;
    }

    public Integer getMaxWireVersion() {
        return maxWireVersion;
    }

    public HelloResult setMaxWireVersion(Integer maxWireVersion) {
        this.maxWireVersion = maxWireVersion;
        return this;
    }

    public Boolean getReadOnly() {
        return readOnly;
    }

    public HelloResult setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public String getCompression() {
        return compression;
    }

    public HelloResult setCompression(String compression) {
        this.compression = compression;
        return this;
    }

    public List<String> getSaslSupportedMechs() {
        return saslSupportedMechs;
    }

    public HelloResult setSaslSupportedMechs(List<String> saslSupportedMechs) {
        this.saslSupportedMechs = saslSupportedMechs;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public HelloResult setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public HelloResult setHosts(List<String> hosts) {
        this.hosts = hosts;
        return this;
    }

    public String getMe() {
        return me;
    }

    public HelloResult setMe(String me) {
        this.me = me;
        return this;
    }

    public String getPrimary() {
        return primary;
    }

    public HelloResult setPrimary(String primary) {
        this.primary = primary;
        return this;
    }

    public String getSetName() {
        return setName;
    }

    public HelloResult setSetName(String setName) {
        this.setName = setName;
        return this;
    }

    public Integer getSetVersion() {
        return setVersion;
    }

    public HelloResult setSetVersion(Integer setVersion) {
        this.setVersion = setVersion;
        return this;
    }

    public Boolean getArbiterOnly() {
        return arbiterOnly;
    }

    public HelloResult setArbiterOnly(Boolean arbiterOnly) {
        this.arbiterOnly = arbiterOnly;
        return this;
    }

    public Boolean getPassive() {
        return passive;
    }

    public HelloResult setPassive(Boolean passive) {
        this.passive = passive;
        return this;
    }

    public Boolean getHidden() {
        return hidden;
    }

    public HelloResult setHidden(Boolean hidden) {
        this.hidden = hidden;
        return this;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public HelloResult setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public MorphiumId getElectionId() {
        return electionId;
    }

    public HelloResult setElectionId(MorphiumId electionId) {
        this.electionId = electionId;
        return this;
    }

    public Map<String, Object> getLastWrite() {
        return lastWrite;
    }

    public HelloResult setLastWrite(Map<String, Object> lastWrite) {
        this.lastWrite = lastWrite;
        return this;
    }

    public boolean isOk() {
        return Double.valueOf(1).equals(ok);
    }

    public Double getOk() {
        return ok;
    }

    public HelloResult setOk(Double ok) {
        this.ok = ok;
        return this;
    }

    public Boolean getHelloOk() {
        return helloOk;
    }

    public HelloResult setHelloOk(Boolean helloOk) {
        this.helloOk = helloOk;
        return this;
    }

    public Boolean getMorphiumServer() {
        return morphiumServer;
    }

    public HelloResult setMorphiumServer(Boolean morphiumServer) {
        this.morphiumServer = morphiumServer;
        return this;
    }

    public Boolean getInMemoryBackend() {
        return inMemoryBackend;
    }

    public HelloResult setInMemoryBackend(Boolean inMemoryBackend) {
        this.inMemoryBackend = inMemoryBackend;
        return this;
    }

    @Override
    public String toString() {
        return "HelloResult{" +
            "helloOk=" + helloOk +
            ", isWritablePrimary=" + isWritablePrimary +
            ", maxBsonObjectSize=" + maxBsonObjectSize +
            ", maxMessageSizeBytes=" + maxMessageSizeBytes +
            ", maxWriteBatchSize=" + maxWriteBatchSize +
            ", localTime=" + localTime +
            ", logicalSessionTimeoutMinutes=" + logicalSessionTimeoutMinutes +
            ", connectionId=" + connectionId +
            ", minWireVersion=" + minWireVersion +
            ", maxWireVersion=" + maxWireVersion +
            ", readOnly=" + readOnly +
            ", compression='" + compression + '\'' +
            ", saslSupportedMechs=" + saslSupportedMechs +
            ", msg='" + msg + '\'' +
            ", hosts=" + hosts +
            ", me='" + me + '\'' +
            ", primary='" + primary + '\'' +
            ", setName='" + setName + '\'' +
            ", setVersion=" + setVersion +
            ", arbiterOnly=" + arbiterOnly +
            ", secondary=" + secondary +
            ", passive=" + passive +
            ", hidden=" + hidden +
            ", tags=" + tags +
            ", electionId=" + electionId +
            ", lastWrite=" + lastWrite +
            ", ok=" + ok +
            '}';
    }
}
