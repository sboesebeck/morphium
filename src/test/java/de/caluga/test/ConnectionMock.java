package de.caluga.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

public class ConnectionMock implements MongoConnection{

    @Override
    public OpMsg readNextMessage(int timeout) throws MorphiumDriverException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        // TODO Auto-generated method stub

    }

    @Override
    public HelloResult connect(MorphiumDriver drv, String host, int port) throws IOException, MorphiumDriverException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId, int batchsize) throws MorphiumDriverException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getConnectedTo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getConnectedToHost() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getConnectedToPort() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public MorphiumDriver getDriver() {
        // TODO Auto-generated method stub
        return new DriverMock();
    }


    @Override
    public int getSourcePort() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isConnected() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Map<String, Object> killCursors(String db, String coll, long... ids) throws MorphiumDriverException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        // TODO Auto-generated method stub
        return null;
    }



    @Override
    public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setCredentials(String authDb, String userName, String password) {
        // TODO Auto-generated method stub

    }

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {
        // TODO Auto-generated method stub

    }
}
