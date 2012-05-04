package de.caluga.morphium;

import org.apache.log4j.Logger;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;


public class ConfigManager implements ShutdownListener{
    private static ConfigManager instance;
    private final Hashtable<String, ConfigElement> configCache;
    private final Hashtable<String, Long> addedAt;
    private int timeout = 1000 * 60 * 60; //one hour
    private boolean running = true;
    private static Logger log = Logger.getLogger(ConfigManager.class);
    private Morphium morphium;



    public void setTimeout(int t) {
        timeout = t;
    }

    public void end() {
        running = false;

    }

    public void reinitSettings() {
        synchronized (addedAt) {
            configCache.clear();
            addedAt.clear();
        }
    }

    public ConfigManager(Morphium m) {
        configCache = new Hashtable<String, ConfigElement>();
        addedAt = new Hashtable<String, Long>();
        morphium=m;
        morphium.addShutdownListener(this);

        Thread t = new Thread() {
            public void run() {
                while (running) {
                    try {
                        Vector<String> toRefresh = new Vector<String>();
                        synchronized (addedAt) {
                            for (String k : addedAt.keySet()) {
                                if (System.currentTimeMillis() - addedAt.get(k) > timeout) {
                                    toRefresh.add(k);
                                }
                            }
                            for (String t : toRefresh) {
                                addedAt.remove(t);
                                configCache.remove(t);
                            }
                        }

                    } catch (Exception e) {
                        log.error("Exception while accessing config cache!", e);
                    }
                    try {
                        sleep(5000);
                    } catch (InterruptedException e) {
//						HiLogger.get().error(e.getMessage(),e);
                    }
                }
            }
        };
        t.setDaemon(true);
        t.start();
    }

    public void addSetting(String k, List<String> v) {
        ConfigElement c = getConfigElement(k);
        c.setListValue(v);
        synchronized (addedAt) {
            addedAt.put(k, System.currentTimeMillis());
        }
        store(k, c);
    }

    public void addSetting(String k, Map<String, String> v) {
        ConfigElement c = getConfigElement(k);
        c.setMapValue(v);

        synchronized (addedAt) {
            addedAt.put(k, System.currentTimeMillis());
        }
        store(k, c);
    }

    public void addSetting(String k, String v) {
        ConfigElement c = getConfigElement(k);
        c.setValue(v);

        store(k, c);
    }

    private void store(String k, ConfigElement c) {

        Query q = morphium.createQueryFor(ConfigElement.class);
        q.f("name").eq(k);
        morphium.setPrivileged();
        List<ConfigElement> lst = morphium.find(q);
        if (lst.size() > 0) {
            c.setId(lst.get(0).getId()); //setting id to enforce update
        }
        synchronized (addedAt) {
            configCache.put(k, c);
            storeSetting(c);
        }
    }

    public ConfigElement getConfigElement(String k) {
        ConfigElement c = loadConfigElement(k);
        if (c == null) {
            c = new ConfigElement();
            c.setName(k);
            synchronized (addedAt) {
                configCache.put(k, c);
                storeSetting(c);
            }
        }
        return c;
    }

    public ConfigElement loadConfigElement(String k) {
        synchronized (addedAt) {
            if (configCache.get(k) != null) {
                return configCache.get(k);
            }
        }
//        ConfigElement c = new ConfigElement();
//        c.setName(k);
        Query<ConfigElement> q = morphium.createQueryFor(ConfigElement.class);
        q=q.f("name").eq(k);
        morphium.setPrivileged();
        List<ConfigElement> lst = morphium.find(q);

        if (lst.size() > 1) {
            log.warn("WARNING: too many entries with name " + k + " taking 1st!");
        }
        if (lst.size() == 0) {
            return null;
        }
        ConfigElement e = lst.get(0);
        if (e.getMapValue() != null) {
            Hashtable<String, String> v = new Hashtable<String, String>();
            for (String key : e.getMapValue().keySet()) {
                String rkey = key.replaceAll("%", "."); //getting "." back
                v.put(rkey, e.getMapValue().get(key));
            }
            e.setMapValue(v);
        }
        synchronized (addedAt) {
            configCache.put(k, e);
            addedAt.put(k, System.currentTimeMillis());
        }
        return e;
    }

    public List<String> getListSetting(String k) {
        ConfigElement ce = loadConfigElement(k);
        if (ce == null) {
            return null;
        }
        return ce.getListValue();
    }

    public Map<String, String> getMapSetting(String k) {
        ConfigElement ce = loadConfigElement(k);
        if (ce == null) {
            return null;
        }
        return ce.getMapValue();
    }


    public void storeSetting(ConfigElement e) {
        if (e.getMapValue() != null) {
            Hashtable<String, String> v = new Hashtable<String, String>();
            for (String key : e.getMapValue().keySet()) {
                String rkey = key.replaceAll("\\.", "%");
                v.put(rkey, e.getMapValue().get(key));
            }
            e.setMapValue(v);
        }
        morphium.setPrivileged();
        morphium.store(e);
    }


    public String getSetting(String k) {
        ConfigElement ce = loadConfigElement(k);
        if (ce == null) {
            return null;
        }
        return ce.getValue();
    }


    @Override
    public void onShutdown(Morphium m) {
        end();
    }
}