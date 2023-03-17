package de.caluga.morphium.messaging;

import java.util.HashMap;

public class StatusInfoListener implements MessageListener<Msg> {

    public final static String messagingThreadpoolstatsKey = "messaging_threadpoolstats";
    public final static String messageListenersbyNameKey = "message_listeners_by_name";
    public final static String globalListenersKey = "global_listeners";
    public final static String morphiumCachestatsKey = "morphium.cachestats";
    public final static String morphiumConfigKey = "morphium.config";

    @Override
    public Msg onMessage(Messaging msg, Msg m) {
        if (m.isAnswer()) return null;
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

        }

        if (level.equals(StatusInfoLevel.ALL) || level.equals(StatusInfoLevel.MORPHIUM_ONLY)) {
            answer.getMapValue().put(morphiumCachestatsKey, msg.getMorphium().getStatistics());
            answer.getMapValue().put(morphiumConfigKey, msg.getMorphium().getConfig().asProperties());
        }
        return answer;
    }

    @Override
    public boolean markAsProcessedBeforeExec() {
        return true;
    }


    public enum StatusInfoLevel {
        ALL, MESSAGING_ONLY, MORPHIUM_ONLY, PING,
    }
}
