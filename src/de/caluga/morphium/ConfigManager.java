package de.caluga.morphium;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 21.06.12
 * Time: 19:26
 * <p/>
 */
public interface ConfigManager extends ShutdownListener {
    public void setTimeout(int t);

    public void end();

    public void reinitSettings();

    public void addSetting(String k, List<String> v);

    public void addSetting(String k, Map<String, String> v);

    public void addSetting(String k, String v);

    public ConfigElement getConfigElement(String k);

    public ConfigElement loadConfigElement(String k);

    public List<String> getListSetting(String k);

    public Map<String, String> getMapSetting(String k);

    public void storeSetting(ConfigElement e);

    public String getSetting(String k);

    public Morphium getMorphium();

    public void setMorphium(Morphium m);

    public void startCleanupThread();

    public List<String> getSettings();

    public List<String> getSettings(String regex);
}
