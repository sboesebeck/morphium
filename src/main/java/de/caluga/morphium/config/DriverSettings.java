package de.caluga.morphium.config;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.wire.PooledDriver;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.List;

@Embedded
public class DriverSettings {

    private MorphiumConfig.CompressionType compressionType = MorphiumConfig.CompressionType.NONE;
    private String uuidRepresentation;
    private boolean retryReads = false;
    private boolean retryWrites = false;
    private int readTimeout = 0;
    private int localThreshold = 15;
    private int maxConnectionIdleTime = 30000;
    private int maxConnectionLifeTime = 600000;


    private String driverName = PooledDriver.driverName;
    private String mongoLogin = null, mongoPassword = null, mongoAuthDb = null;
    @Transient
    private ReadPreference defaultReadPreference = ReadPreference.nearest();
    @Transient
    private String defaultReadPreferenceType;
}
