package de.caluga.morphium;/**
 * Created by stephan on 17.07.15.
 */

/**
 *
 **/
public class Log4JLoggerDelegate implements LoggerDelegate {

    @Override
    public void log(String name, int lv, String msg, Throwable t) {
        try {
            Object logger = Class.forName("org.log4j.Logger").getMethod("getLogger").invoke(null, name);

            String mname = "";
            switch (lv) {
                case 0:
                    break;
                case 1:
                    mname = "fatal";
                    break;
                case 2:
                    mname = "error";
                    break;
                case 3:
                    mname = "warn";
                    break;
                case 4:
                    mname = "info";
                    break;
                case 5:
                    mname = "debug";
                    break;
                default:
                    mname = "unknown";
                    break;
            }

            if (mname.isEmpty()) {
                return;
            }
            if (!((Boolean) logger.getClass().getMethod("is" + capitalize(mname) + "Enabled").invoke(logger))) {
                return;
            }
            logger.getClass().getMethod(mname, String.class, Throwable.class).invoke(logger, msg, t);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private String capitalize(String n) {
        return n.substring(0, 1).toUpperCase() + n.substring(1);
    }

}
