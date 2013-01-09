package de.caluga.morphium;

import de.caluga.morphium.query.Query;
import org.apache.log4j.Logger;

import java.util.*;

@SuppressWarnings("UnusedDeclaration")
public class ConfigManagerImpl implements ConfigManager {
    private static ConfigManager instance;
    private final Hashtable<String, ConfigElement> configCache;
    private final Hashtable<String, Long> addedAt;
    private int timeout = 1000 * 60 * 60; //one hour
    private boolean running = true;
    private static Logger log = Logger.getLogger(ConfigManagerImpl.class);
    private Morphium morphium;


    @Override
    public void setTimeout(int t) {
        timeout = t;
    }

    @Override
    public void end() {
        running = false;

    }

    @Override
    public void reinitSettings() {
        synchronized (addedAt) {
            configCache.clear();
            addedAt.clear();
        }
    }

    public Morphium getMorphium() {
        return morphium;
    }

    public void setMorphium(Morphium m) {
        morphium = m;
        morphium.addShutdownListener(this);


    }


    public void startCleanupThread() {
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

    @Override
    public List<String> getSettings() {
        return getSettings(null);
    }

    @Override
    public List<String> getSettings(String regex) {
        Query<ConfigElement> q = morphium.createQueryFor(ConfigElement.class);
        if (regex != null) {
            q.f("name").matches(regex);
        }
        List<String> ret = new ArrayList<String>();
        List<ConfigElement> el = q.asList();
        for (ConfigElement e : el) {
            ret.add(e.getName());
            if (configCache.containsKey(e.getName())) {
                continue;
            }
            configCache.put(e.getName(), e);
            addedAt.put(e.getName(), System.currentTimeMillis());
        }
        return ret;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public ConfigManagerImpl() {
        configCache = new Hashtable<String, ConfigElement>();
        addedAt = new Hashtable<String, Long>();

    }

    @Override
    public void addSetting(String k, List<String> v) {
        ConfigElement c = getConfigElement(k);
        c.setListValue(v);
        synchronized (addedAt) {
            addedAt.put(k, System.currentTimeMillis());
        }
        store(k, c);
    }

    @Override
    public void addSetting(String k, Map<String, String> v) {
        ConfigElement c = getConfigElement(k);
        c.setMapValue(v);

        synchronized (addedAt) {
            addedAt.put(k, System.currentTimeMillis());
        }
        store(k, c);
    }

    @Override
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

    @Override
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

    @Override
    public ConfigElement loadConfigElement(String k) {
        synchronized (addedAt) {
            if (configCache.get(k) != null) {
                return configCache.get(k);
            }
        }
//        ConfigElement c = new ConfigElement();
//        c.setName(k);
        Query<ConfigElement> q = morphium.createQueryFor(ConfigElement.class);
        q = q.f("name").eq(k);
        morphium.setPrivileged();
        List<ConfigElement> lst = morphium.find(q);

        if (lst.size() > 1) {
            log.warn("WARNING: too many entries with name " + k + " taking 1st!");
        }
        if (lst.size() == 0) {
            return null;
        }
        ConfigElement e = lst.get(0);
        updateLocal(e);
        synchronized (addedAt) {
            configCache.put(k, e);
            addedAt.put(k, System.currentTimeMillis());
        }
        return e;
    }

    private void updateLocal(ConfigElement e) {
        if (e.getMapValue() != null) {
            Hashtable<String, String> v = new Hashtable<String, String>();
            for (String key : e.getMapValue().keySet()) {
                String rkey = key.replaceAll("%", "."); //getting "." back
                v.put(rkey, e.getMapValue().get(key));
            }
            e.setMapValue(v);
        }
    }

    @Override
    public List<String> getListSetting(String k) {
        ConfigElement ce = loadConfigElement(k);
        if (ce == null) {
            return null;
        }
        return ce.getListValue();
    }

    @Override
    public Map<String, String> getMapSetting(String k) {
        ConfigElement ce = loadConfigElement(k);
        if (ce == null) {
            return null;
        }
        return ce.getMapValue();
    }


    @Override
    public void storeSetting(ConfigElement e) {
        updateLocal(e);
        morphium.setPrivileged();
        morphium.store(e);
    }


    @Override
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