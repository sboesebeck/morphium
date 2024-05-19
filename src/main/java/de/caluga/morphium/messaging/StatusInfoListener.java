package de.caluga.morphium.messaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.MorphiumDriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusInfoListener implements MessageListener<Msg> {
    private Logger log = LoggerFactory.getLogger(StatusInfoListener.class);
    public final static String messagingThreadpoolstatsKey = "messaging_threadpoolstats";
    public final static String messageListenersbyNameKey = "message_listeners_by_name";
    public final static String globalListenersKey = "global_listeners";
    public final static String morphiumCachestatsKey = "morphium.cachestats";
    public final static String morphiumConfigKey = "morphium.config";
    public final static String morphiumDriverStatsKey = "morphium.driver.stats";
    public final static String morphiumDriverConnections = "morphium.driver.connections";
    public final static String morphiumDriverReplstatKey = "morphium.driver.replicaset_status";

    //    private Map<String, InternalCommand> commands = new HashMap<>();

    public StatusInfoListener() {
        //        commands.put("unpause", new InternalCommand("pause") {
        //            public void exec(Messaging msg, Msg answer, Object params) {
        //                if (params != null) {
        //                    if (params instanceof List) {
        //                        var lst = (List<String>)params;
        //
        //                        for (String n : lst) {
        //                            msg.unpauseProcessingOfMessagesNamed(n);
        //                        }
        //                    } else {
        //                        var n=params.toString();
        //                        msg.unpauseProcessingOfMessagesNamed(n);
        //
        //                    }
        //                } else {
        //                    //pause ALL
        //                    var lst=msg.getPausedMessageNames();
        //                    for (String n:lst){
        //                        msg.unpauseProcessingOfMessagesNamed(n);
        //                    }
        //                }
        //            }
        //        });
        //        commands.put("pause", new InternalCommand("pause") {
        //            public void exec(Messaging msg, Msg answer, Object params) {
        //                if (params != null) {
        //                    if (params instanceof List) {
        //                        var lst = (List<String>)params;
        //
        //                        for (String n : lst) {
        //                            msg.pauseProcessingOfMessagesNamed(n);
        //                        }
        //                    } else {
        //                        var n=params.toString();
        //                        msg.pauseProcessingOfMessagesNamed(n);
        //
        //                    }
        //                } else {
        //                    //pause ALL
        //                    var lst=new ArrayList<String>(msg.getListenerNames().keySet());
        //                    lst.addAll(msg.getGlobalListeners());
        //                    for (String n:lst){
        //                        msg.pauseProcessingOfMessagesNamed(n);
        //                    }
        //                }
        //            }
        //        });
    }

    @Override
    public Msg onMessage(Messaging msg, Msg m) {
        if (m.isAnswer()) {
            return null;
        }

        log.debug("Preparing status info... " + m.getMsgId());
        long tripDur = System.currentTimeMillis() - m.getTimestamp();
        Msg answer = m.createAnswerMsg();
        answer.setMapValue(new HashMap<>());
        StatusInfoLevel level = StatusInfoLevel.MESSAGING_ONLY;

        if (m.getValue() != null) {
            for (StatusInfoLevel l : StatusInfoLevel.values()) {
                if (l.name().equals(m.getValue())) {
                    level = StatusInfoLevel.valueOf(m.getValue());
                    break;
                }
            }
        }

        if (level.equals(StatusInfoLevel.ALL) || level.equals(StatusInfoLevel.MESSAGING_ONLY)) {
            if (msg.isMultithreadded()) {
                answer.getMapValue().put(messagingThreadpoolstatsKey, msg.getThreadPoolStats());
            }

            answer.getMapValue().put(messageListenersbyNameKey, msg.getListenerNames());
            answer.getMapValue().put(globalListenersKey, msg.getGlobalListeners());
            answer.getMapValue().put("messaging.changestream", msg.isUseChangeStream());
            answer.getMapValue().put("messaging.multithreadded", msg.isMultithreadded());
            answer.getMapValue().put("messaging.window_size", msg.getWindowSize());
            answer.getMapValue().put("messaging.pause", msg.getPause());
            answer.getMapValue().put("messaging.time_till_recieved", tripDur);
        }

        if (level.equals(StatusInfoLevel.ALL) || level.equals(StatusInfoLevel.MORPHIUM_ONLY)) {
            answer.getMapValue().put(morphiumCachestatsKey, msg.getMorphium().getStatistics());
            answer.getMapValue().put(morphiumConfigKey, msg.getMorphium().getConfig().asProperties());
            answer.getMapValue().put(morphiumDriverStatsKey, msg.getMorphium().getDriver().getDriverStats());
            answer.getMapValue().put(morphiumDriverConnections, msg.getMorphium().getDriver().getNumConnectionsByHost());

            try {
                answer.getMapValue().put(morphiumDriverReplstatKey, msg.getMorphium().getDriver().getReplsetStatus());
            } catch (MorphiumDriverException e) {
                answer.getMapValue().put(morphiumDriverReplstatKey, "could not get replicaset status: " + e.getMessage());
            }
        }

        if (level.equals(StatusInfoLevel.ALL)) {
            //alternative morphium stats
            int i = 0;

            for (var alternativeMorphium : msg.getMorphium().getAlternativeMorphiums()) {
                i++;
                Map<String, Object> stats = new HashMap<>();
                stats.put(morphiumCachestatsKey, alternativeMorphium.getStatistics());
                stats.put(morphiumConfigKey, alternativeMorphium.getConfig().asProperties());
                stats.put(morphiumDriverStatsKey, alternativeMorphium.getDriver().getDriverStats());
                stats.put(morphiumDriverConnections, alternativeMorphium.getDriver().getNumConnectionsByHost());

                try {
                    stats.put(morphiumDriverReplstatKey, alternativeMorphium.getDriver().getReplsetStatus());
                } catch (MorphiumDriverException e) {
                    stats.put(morphiumDriverReplstatKey, "could not get replicaset status: " + e.getMessage());
                }

                answer.getMapValue().put("alternativeMorphium" + i, stats);
            }
        }

        log.info(" status info done... " + m.getMsgId());
        return answer;
    }

    @Override
    public boolean markAsProcessedBeforeExec() {
        return true;
    }

    public enum StatusInfoLevel {
        ALL, MESSAGING_ONLY, MORPHIUM_ONLY, PING,
    }

    public static abstract class InternalCommand {
        private String name;

        public InternalCommand(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }


        public abstract void exec(Messaging msg, Msg answer, Object params);
    }
}
