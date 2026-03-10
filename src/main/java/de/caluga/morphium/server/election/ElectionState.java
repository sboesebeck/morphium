package de.caluga.morphium.server.election;

/**
 * Represents the possible states of a node in the election protocol.
 * Based on the Raft consensus algorithm state machine.
 */
public enum ElectionState {
    /**
     * Following a leader, cannot accept writes.
     * This is the initial state when a node starts.
     * Transitions to CANDIDATE if election timeout expires without receiving heartbeat.
     */
    FOLLOWER,

    /**
     * Requesting votes from other nodes, no leader yet.
     * Transitions to LEADER if majority votes received.
     * Transitions to FOLLOWER if higher term discovered or another leader elected.
     */
    CANDIDATE,

    /**
     * Accepted as leader by majority, can accept writes.
     * Sends periodic heartbeats to maintain leadership.
     * Transitions to FOLLOWER if higher term discovered.
     */
    LEADER
}
