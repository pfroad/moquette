package io.moquette.persistence.redis;

/**
 * Created by ryan on 5/2/17.
 */
public class KeyConstants {
    // maps clientID -> guid
    public static final String RETAINED_STORE = "mqtt:retained:topic";
    // maps guid to message, it's message store
    public static final String PERSISTENT_MSG_STORE = "mqtt:persistent:GUID";

    public static final String SUBSCRIPTIONS = "mqtt:subscriptions:clientId";

    public static final String PERSISTENT_SESSIONS = "mqtt:sessions:clientId";
}
