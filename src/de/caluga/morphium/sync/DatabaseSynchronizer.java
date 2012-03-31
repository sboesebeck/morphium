/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.caluga.morphium.sync;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Query;
import de.caluga.morphium.StorageAdapter;
import org.apache.log4j.Logger;

import java.net.NetworkInterface;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Diese Klasse hängt sich an den DBManager und wird über alle Änderungen informiert
 * Änderungen werden zusammen mit der (hoffentlich eindeutigen App-ID) in die Tabelle
 * DbSync gespeichert.
 * Ein Thread wird gestartet, der periodisch checkt, ob einträge <b>ohne eigene ID</b> in der liste sind
 * Falls das ist, wird der entsprechende Cache gelöscht!
 *
 * @author stephan
 */
public class DatabaseSynchronizer extends Thread {

    private String id = "";
    private boolean running = true;
    private int sleepTimer = 500;
    private LinkedList<SyncListener> listeners;
    private HashMap<Class<? extends Object>, LinkedList<SyncListener>> listenerByType;
    private static final Logger log = Logger.getLogger(DatabaseSynchronizer.class);

    //so wäre es unsinnig, aber in Verbindung mit der MAC-Adresse eindeutig!


    /**
     * registriert sich auch beim DBManager
     * startet den Bg-Thread für den Datenbank-Check
     */
    public DatabaseSynchronizer() {
        String mac = "";
        try {

            for (NetworkInterface ni : Collections.list(
                    NetworkInterface.getNetworkInterfaces())) {
                byte[] hardwareAddress = ni.getHardwareAddress();

                if (hardwareAddress != null) {
                    for (int i = 0; i < hardwareAddress.length; i++) {
                        mac += String.format((i == 0 ? "" : "") + "%02X", hardwareAddress[i]);
                    }
                    if (mac.length() > 0 && !ni.isLoopback()) {
                        break;
                    }
                }
            }
            //System.out.println("Result:; "+result);


        } catch (Exception e) {
            log.warn("Error:" + e.getMessage(), e);
        }
        long system_id = System.currentTimeMillis();

        id = mac + "-" + system_id;

        setDaemon(true);
        listeners = new LinkedList<SyncListener>();
        listenerByType = new HashMap<Class<? extends Object>, LinkedList<SyncListener>>();


        Morphium.get().addListener(new StorageAdapter() {

            @Override
            public void postStore(Object r) {
                if (r instanceof DbSync) {
                    return;
                }
//                System.out.println("OnStore!");
                //Daten gespeichert, action in die Sync-Tabelle
                DbSync d = new DbSync();
                d.setAction(ActionEnum.STORE.toString());
                d.addAppId(id);
                d.setDataType(r.getClass().getName());
                Morphium.get().store(d);

            }

            @Override
            public void postRemove(Object r) {
                if (r instanceof DbSync) {
                    return;
                }
                //Daten gespeichert, action in die Sync-Tabelle
                DbSync d = new DbSync();
                d.setAction(ActionEnum.DELETE.toString());
                d.addAppId(id);
                d.setDataType(r.getClass().getName());
                Morphium.get().store(d);
            }
        });
        //ältere Einträge löschen, sollten unwichtig sein
//
        Query<DbSync> q = Morphium.get().createQueryFor(DbSync.class);
        q.f("created").gte(System.currentTimeMillis() - 15000);
        Morphium.get().delete(q);

        //start the Thread!
        start();

    }

    public void stopRunning() {
        running = false;
    }

    public void removeListener(Class<? extends Object> type, SyncListener l) {
        if (!listenerByType.containsKey(type)) {
            //listenerByType.put(type, new LinkedList<SyncListener>());
            return;
        }
        LinkedList<SyncListener> lst = listenerByType.get(type);
        lst.remove(l);
    }

    public void addListener(Class<? extends Object> type, SyncListener l) {
        if (!listenerByType.containsKey(type)) {
            listenerByType.put(type, new LinkedList<SyncListener>());
        }
        LinkedList<SyncListener> lst = listenerByType.get(type);
        lst.add(l);
    }

    public void addListener(SyncListener l) {
        listeners.add(l);
    }

    public void remove(SyncListener l) {
        listeners.remove(l);
    }

    public void run() {
        while (running) {
            //nach einträgen suchen, die diese AppID noch nicht tragen
            //DbSync db = new DbSync();
            Query<DbSync> dbq = Morphium.get().createQueryFor(DbSync.class);
            //TODO: fix it
            //dbq.f("app_ids").hasThisOne(id);

            List<DbSync> dbs = dbq.asList();
            if (dbs != null && dbs.size() > 0) {
                //Found entry
                for (DbSync d : dbs) {
                    try {
                        //process it
                        String type = d.getDataType();
                        String action = d.getAction();
                        //Caches löschen
                        Class<? extends Object> t = (Class<? extends Object>) Class.forName(d.getDataType());
                        Morphium.get().clearCachefor(t);

                        //Selbst wenn die Transaktion fehl schlägt, kein Problem, dann
                        // wird der cache eben noch mal gelöscht
                        // so ist die Belastung für die Datenbank deutlich geringer
                        d.addAppId(id);
                        Morphium.get().store(d);

                        fireSyncEvent(t, action);
                    } catch (ClassNotFoundException ex) {
                        log.fatal("Calss not found?!?!? " + d.getDataType());
                    }


                }
            }
            try {
                sleep(getSleepTimer());
            } catch (InterruptedException ex) {
                Logger.getLogger(DatabaseSynchronizer.class).fatal(ex);
                return;
            }
        }
    }

    private void fireSyncEvent(Class<? extends Object> type, String action) {
        for (SyncListener l : listeners) {
            l.syncEvent(type, action);
        }

        if (listenerByType.containsKey(type)) {
            for (SyncListener l : listenerByType.get(type)) {
                l.syncEvent(type, action);
            }
        }
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the sleepTimer
     */
    public int getSleepTimer() {
        return sleepTimer;
    }

    /**
     * @param sleepTimer the sleepTimer to set
     */
    public void setSleepTimer(int sleepTimer) {
        this.sleepTimer = sleepTimer;
    }
}
