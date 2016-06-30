package de.caluga.morphium;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by stephan on 25.04.15.
 */
@SuppressWarnings({"WeakerAccess", "DefaultFileTemplate"})
public class Logger {
    public static final int defaultLevel = 1;
    public static final boolean defaultSynced = false;
    public static final String defaultFile = "-";
    private final String prfx;
    private final DateFormat df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS");
    private int level = 5;
    private String file;
    private PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out));
    private boolean synced = false;
    private boolean close = false;
    private LoggerDelegate delegate = new DefaultLoggerDelegate();

    public Logger(String name) {

        prfx = name;

        String v = getSetting("log.level");
        if (getSetting("log.level." + name) != null) {
            v = getSetting("log.level." + name);
        }

        if (v != null) {
            level = Integer.parseInt(v);
        } else {
            level = defaultLevel;
        }

        v = getSetting("log.file");
        if (getSetting("log.file." + name) != null) {
            v = getSetting("log.file." + name);
        }
        if (v == null) {
            v = defaultFile;
        }

        file = v;
        if (v.startsWith("class:")) {
            try {
                delegate = (LoggerDelegate) Class.forName(v.substring(v.indexOf(":") + 1)).newInstance();
            } catch (Exception e) {
                e.printStackTrace();
                out = new PrintWriter(new OutputStreamWriter(System.out));
            }
        } else {

            switch (v) {
                case "-":
                case "STDOUT":
                    out = new PrintWriter(new OutputStreamWriter(System.out));
                    close = false;
                    break;
                case "STDERR":
                    out = new PrintWriter(new OutputStreamWriter(System.err));
                    close = false;
                    break;
                default:
                    try {
                        out = new PrintWriter(new BufferedWriter(new FileWriter(v, true)));
                        close = true;
                    } catch (IOException e) {
                        error(null, e);
                    }
                    break;
            }
        }

        v = getSetting("log.synced");
        if (getSetting("log.synced." + name) != null) {
            v = getSetting("log.synced." + name);
        }
        if (v != null) {
            synced = v.equals("true");
        } else {
            synced = defaultSynced;
        }

        v = getSetting("log.delegate");
        if (getSetting("log.delegate." + name) != null) {
            v = getSetting("log.delegate." + name);
        }
        if (v != null) {
            switch (v) {
                case "log4j":
                    delegate = new Log4JLoggerDelegate();
                    break;
                case "jul":
                    delegate = new JavaUtilLoggingDelegate();
                    break;
                default:
                    try {
                        delegate = (LoggerDelegate) Class.forName(v).newInstance();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
            }
        }


        //        info("Logger " + name + " instanciated: Level: " + level + " Synced: " + synced + " file: " + file);
    }

    public Logger(Class cls) {
        this(cls.getName());
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        out.flush();
        if (close) {
            out.close();
        }

    }

    public String getFile() {
        return file;
    }

    @SuppressWarnings("unused")
    public void setFile(String v) {
        if (!file.equals("-") && !file.equals("STDOUT") && !file.equals("STDERR ")) {
            out.flush();
            out.close();
        }
        this.file = v;
        if (file == null || file.equals("-") || file.equalsIgnoreCase("STDOUT")) {
            out = new PrintWriter(new OutputStreamWriter(System.out));
        } else if (file.equalsIgnoreCase("STDERR")) {
            out = new PrintWriter(new OutputStreamWriter(System.err));
        } else {
            try {
                out = new PrintWriter(new BufferedWriter(new FileWriter(v, true)));
            } catch (IOException e) {
                error(null, e);
            }
        }
    }

    private String getSetting(String s) {
        String v = null;
        s = "morphium." + s;
        if (System.getenv(s.replaceAll("\\.", "_")) != null) {
            v = System.getenv(s.replaceAll("\\.", "_"));
        }
        if (System.getProperty(s) != null) {
            v = System.getProperty(s);
        }
        if (v == null) {
            //no setting yet, looking for prefixes
            int lng = 0;
            for (Map.Entry<Object, Object> p : System.getProperties().entrySet()) {
                if (s.startsWith(p.getKey().toString()) && p.getKey().toString().length() > lng) {
                    //keeping the longest prefix
                    //if s== log.level.de.caluga.morphium.ObjetMapperImpl
                    // property: morphium.log.level.de.caluga.morphium=5
                    // property: morphium.log.level.de.caluga=0
                    // => keep only the longer one (5)
                    lng = p.getKey().toString().length();
                    v = p.getValue().toString();

                }
            }
        }

        return v;
    }

    public void setLevel(int lv) {
        level = lv;
    }

    public int getLevel() {
        return level;
    }

    @SuppressWarnings({"EmptyMethod", "unused", "UnusedParameters"})
    public void setLevel(Object o) {
        //ignore
    }

    @SuppressWarnings({"SameReturnValue", "unused", "UnusedParameters"})
    public boolean isEnabledFor(Object o) {
        return true;
    }

    public boolean isDebugEnabled() {
        return level >= 5;
    }

    @SuppressWarnings("unused")
    public boolean isInfoEnabled() {
        return level >= 4;
    }

    @SuppressWarnings("unused")
    public boolean isWarnEnabled() {
        return level >= 3;
    }

    @SuppressWarnings("unused")
    public boolean isErrorEnabled() {
        return level >= 2;
    }

    @SuppressWarnings("unused")
    public boolean isFatalEnabled() {
        return level >= 1;
    }

    @SuppressWarnings("unused")
    public void info(Object msg) {
        info(msg.toString(), null);
    }

    public void info(String msg) {
        info(msg, null);
    }

    @SuppressWarnings("unused")
    public void info(Throwable t) {
        info(null, t);
    }

    public void info(String msg, Throwable t) {
        doLog(4, msg, t);

    }

    @SuppressWarnings("unused")
    public void debug(Object msg) {
        debug(msg.toString());
    }

    public void debug(String msg) {
        debug(msg, null);
    }

    @SuppressWarnings("unused")
    public void debug(Throwable t) {
        debug(null, t);
    }

    public void debug(String msg, Throwable t) {
        doLog(5, msg, t);

    }

    @SuppressWarnings("unused")
    public void warn(Object msg) {
        warn(msg.toString(), null);
    }

    public void warn(String msg) {
        warn(msg, null);
    }

    @SuppressWarnings("unused")
    public void warn(Throwable t) {
        warn(null, t);
    }

    public void warn(String msg, Throwable t) {
        doLog(3, msg, t);

    }

    @SuppressWarnings("unused")
    public void error(Object msg) {
        error(msg.toString(), null);
    }

    public void error(String msg) {
        error(msg, null);
    }

    public void error(Throwable t) {
        error(null, t);
    }

    public void error(String msg, Throwable t) {
        doLog(2, msg, t);

    }

    @SuppressWarnings("unused")
    public void fatal(Object msg) {
        fatal(msg.toString(), null);
    }

    public void fatal(String msg) {
        fatal(msg, null);
    }

    public void fatal(Throwable t) {
        fatal(null, t);
    }

    public void fatal(String msg, Throwable t) {
        doLog(1, msg, t);
    }


    @SuppressWarnings("unused")
    public boolean isSynced() {
        return synced;
    }

    public void setSynced(boolean synced) {
        this.synced = synced;
    }

    private void doLog(int lv, String msg, Throwable t) {
        if (level >= lv) {
            delegate.log(prfx, lv, msg, t);
        }
    }

    public PrintWriter getOutput() {
        return out;
    }

    private class DefaultLoggerDelegate implements LoggerDelegate {

        @Override
        public void log(String name, int lv, String msg, Throwable t) {
            out.print(df.format(new Date()));
            out.print(":");
            switch (lv) {
                case 5:
                    out.print(" DEBUG ");
                    break;
                case 4:
                    out.print(" INFO ");
                    break;
                case 3:
                    out.print(" WARN ");
                    break;
                case 2:
                    out.print(" ERROR ");
                    break;
                case 1:
                    out.print(" FATAL ");
                    break;
                default:
                    return;
            }
            out.print("[");
            StackTraceElement[] st = new Exception().getStackTrace();
            int idx = 0;
            while ((st[idx].getClassName().equals(this.getClass().getName()) || st[idx].getClassName().equals(Logger.class.getName())) && idx <= st.length) {
                idx++;
            }
            out.print(st[idx].getClassName());
            out.print(".");
            out.print(st[idx].getMethodName());
            out.print("():");
            out.print(st[idx].getLineNumber());
            out.print("\t");
            if (msg != null) {
                out.print(msg);
            }
            out.println();
            if (t != null) {
                t.printStackTrace();
            }
            if (synced) {
                out.flush();
            }
        }
    }
}
