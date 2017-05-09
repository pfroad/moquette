package io.moquette.persistence.redis;

import com.alibaba.fastjson.JSON;
import io.moquette.persistence.PersistentSession;
import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.Utils;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by ryan on 4/27/17.
 */
public class RedisSessionStore implements ISessionsStore {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSessionStore.class);

    private JedisPool jedisPool;
    private final IMessagesStore m_messagesStore;

    public RedisSessionStore(JedisPool jedisPool, IMessagesStore messagesStore) {
        this.jedisPool = jedisPool;
        this.m_messagesStore = messagesStore;
    }

    @Override
    public void initStore() {

    }

    @Override
    public void addNewSubscription(Subscription newSubscription) {
        LOG.info("Adding new subscription. ClientId={}, topics={}", newSubscription.getClientId(),
            newSubscription.getTopicFilter());
        final String clientID = newSubscription.getClientId();
        final Jedis jedis = jedisPool.getResource();

        try {
            jedis.hset((KeyConstants.SUBSCRIPTIONS + clientID).getBytes(), JSON.toJSONBytes(newSubscription.getTopicFilter()), JSON.toJSONBytes(newSubscription));
            if (LOG.isTraceEnabled()) {
                final Map<byte[], byte[]> map = jedis.hgetAll((KeyConstants.SUBSCRIPTIONS + clientID).getBytes());

                final Map<Topic, Subscription> subscriptionsMap = deserSubscriptions(map);
                LOG.trace("Subscription has been added. ClientId={}, topics={}, clientSubscriptions={}",
                    newSubscription.getClientId(), newSubscription.getTopicFilter(),
                    subscriptionsMap);
            }
        } catch (Exception e) {
            LOG.error("Failed to add subscription.", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private Map<Topic, Subscription> deserSubscriptions(Map<byte[], byte[]> subscriptions) {
        final Map<Topic, Subscription> subscriptionsMap = new HashMap<>();
        subscriptions.entrySet().stream().forEach(it -> {
            subscriptionsMap.put(JSON.parseObject(it.getKey(), Topic.class), JSON.parseObject(it.getValue(), Subscription.class));
        });
        return subscriptionsMap;
    }

    @Override
    public void removeSubscription(Topic topicFilter, String clientID) {
        LOG.info("Removing subscription. ClientId={}, topics={}", clientID, topicFilter);

        Jedis jedis = jedisPool.getResource();

        try{
            if (jedis.exists(KeyConstants.SUBSCRIPTIONS + clientID)) {
                jedis.hdel(JSON.toJSONBytes(KeyConstants.SUBSCRIPTIONS + clientID), JSON.toJSONBytes(topicFilter));
            }

            if (LOG.isDebugEnabled()) {
                final Map<byte[], byte[]> map = jedis.hgetAll((KeyConstants.SUBSCRIPTIONS + clientID).getBytes());

                final Map<Topic, Subscription> subscriptionsMap = deserSubscriptions(map);
                LOG.trace("Subscription has been added. ClientId={}, topics={}, clientSubscriptions={}",
                    clientID, topicFilter, subscriptionsMap);
            }
        } catch (Exception e) {
            LOG.error("Failed to remove subscription.", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public void wipeSubscriptions(String clientID) {
        LOG.info("Wiping subscriptions. ClientId={}", clientID);

        Jedis jedis = jedisPool.getResource();

        try{
            if (jedis.exists(KeyConstants.SUBSCRIPTIONS + clientID)) {
                jedis.del(KeyConstants.SUBSCRIPTIONS + clientID);
            }

            if (LOG.isDebugEnabled()) {
                final Map<byte[], byte[]> map = jedis.hgetAll((KeyConstants.SUBSCRIPTIONS + clientID).getBytes());
                final Map<Topic, Subscription> subscriptionsMap = deserSubscriptions(map);
                LOG.trace("Subscription has been added. ClientId={}, clientSubscriptions={}", clientID, subscriptionsMap);
            }
        } catch (Exception e) {
            LOG.error("Failed to remove subscription.", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<ClientTopicCouple> listAllSubscriptions() {
        LOG.info("Retrieving existing subscriptions...");
        final List<ClientTopicCouple> allSubscriptions = new ArrayList<>();

        Jedis jedis = jedisPool.getResource();

        try{
            Set<String> keySet = jedis.hkeys(KeyConstants.PERSISTENT_SESSIONS);

            for (String clientID : keySet) {
                final Map<byte[], byte[]> map = jedis.hgetAll((KeyConstants.SUBSCRIPTIONS + clientID).getBytes());
                final Map<Topic, Subscription> subscriptionsMap = deserSubscriptions(map);

                for (Topic topicFilter : subscriptionsMap.keySet()) {
                    allSubscriptions.add(new ClientTopicCouple(clientID, topicFilter));
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.trace("The existing subscriptions have been retrieved. Result={}", allSubscriptions);
            }
        } catch (Exception e) {
            LOG.error("Failed to remove subscription.", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

        return allSubscriptions;
    }

    @Override
    public Subscription getSubscription(ClientTopicCouple couple) {
        Jedis jedis = jedisPool.getResource();

        try{
            final Map<byte[], byte[]> map = jedis.hgetAll((KeyConstants.SUBSCRIPTIONS + couple.clientID).getBytes());
            final Map<Topic, Subscription> subscriptionsMap = deserSubscriptions(map);
            LOG.info("Retrieving subscriptions. ClientId={}, subscriptions={}", couple.clientID, subscriptionsMap);
            return subscriptionsMap.get(couple.topicFilter);
        } catch (Exception e) {
            LOG.error("Failed to remove subscription.", e);
            return null;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public List<Subscription> getSubscriptions() {
        LOG.info("Retrieving existing subscriptions...");
        List<Subscription> subscriptions = new ArrayList<>();

        Jedis jedis = jedisPool.getResource();

        try{
            Set<String> keySet = jedis.hkeys(KeyConstants.PERSISTENT_SESSIONS);

            keySet.stream().forEach(clientID -> {
                final Map<byte[], byte[]> map = jedis.hgetAll((KeyConstants.SUBSCRIPTIONS + clientID).getBytes());
                final Map<Topic, Subscription> subscriptionsMap = deserSubscriptions(map);
                subscriptions.addAll(subscriptionsMap.values());
            });
        } catch (Exception e) {
            LOG.error("Failed to remove subscription.", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        LOG.debug("The existing subscriptions have been retrieved. Result={}", subscriptions);
        return subscriptions;
    }

    @Override
    public boolean contains(String clientID) {
        Jedis jedis = jedisPool.getResource();

        try{
            return jedis.exists(KeyConstants.SUBSCRIPTIONS + clientID);
        } catch (Exception e) {
            LOG.error("Failed to remove subscription.", e);
            return false;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public ClientSession createNewSession(String clientID, boolean cleanSession) {

        Jedis jedis = jedisPool.getResource();

        try{
            Set<String> keySet = jedis.hkeys(KeyConstants.PERSISTENT_SESSIONS);

            if (keySet.contains(clientID)) {
                LOG.error("Unable to create a new session: the client ID is already in use. ClientId={}, cleanSession={}",
                    clientID, cleanSession);
                throw new IllegalArgumentException("Can't create a session with the ID of an already existing" + clientID);
            }

            LOG.info("Creating new session. ClientId={}, cleanSession={}", clientID, cleanSession);
            jedis.hsetnx(KeyConstants.PERSISTENT_SESSIONS.getBytes(), clientID.getBytes(), JSON.toJSONBytes(new PersistentSession(cleanSession)));

            LOG.info("Creating new session. ClientId={}, cleanSession={}", clientID, cleanSession);
        } catch (Exception e) {
            LOG.error("Failed to remove subscription.", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

        return new ClientSession(clientID, m_messagesStore, this, cleanSession);
    }

    @Override
    public ClientSession sessionForClient(String clientID) {
        LOG.info("Retrieving session. ClientId={}", clientID);
        if (!m_persistentSessions.containsKey(clientID)) {
            LOG.warn("The session does not exist. ClientId={}", clientID);
            return null;
        }

        PersistentSession storedSession = m_persistentSessions.get(clientID);
        return new ClientSession(clientID, m_messagesStore, this, storedSession.cleanSession);
    }

    @Override
    public Collection<ClientSession> getAllSessions() {
        Collection<ClientSession> result = new ArrayList<>();
        for (Map.Entry<String, PersistentSession> entry : m_persistentSessions.entrySet()) {
            result.add(new ClientSession(entry.getKey(), m_messagesStore, this, entry.getValue().cleanSession));
        }
        return result;
    }

    @Override
    public void updateCleanStatus(String clientID, boolean cleanSession) {
        LOG.info("Updating cleanSession flag. ClientId={}, cleanSession={}", clientID, cleanSession);
        m_persistentSessions.put(clientID, new PersistentSession(cleanSession));
    }

    /**
     * Return the next valid packetIdentifier for the given client session.
     */
    @Override
    public int nextPacketID(String clientID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generating next packet ID. ClientId={}", clientID);
        }
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient == null) {
            int nextPacketId = 1;
            inFlightForClient = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
            inFlightForClient.add(nextPacketId);
            this.m_inFlightIds.put(clientID, inFlightForClient);
            return nextPacketId;

        }

        int maxId = inFlightForClient.isEmpty() ? 0 : Collections.max(inFlightForClient);
        int nextPacketId = (maxId % 0xFFFF) + 1;
        inFlightForClient.add(nextPacketId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("The next packet ID has been generated. ClientId={}, result={}", clientID, nextPacketId);
        }
        return nextPacketId;
    }

    @Override
    public void inFlightAck(String clientID, int messageID) {
        LOG.debug("Acknowledging inflight message. ClientId={}, messageId={}", clientID, messageID);
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            LOG.warn("Unable to retrieve inflight message record. ClientId={}, messageId={}", clientID, messageID);
            return;
        }
        m.remove(messageID);

        // remove from the ids store
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient != null) {
            inFlightForClient.remove(messageID);
        }
    }

    @Override
    public void inFlight(String clientID, int messageID, MessageGUID guid) {
        LOG.debug("Storing inflight message. ClientId={}, messageId={}, guid={}", clientID, messageID, guid);
        ConcurrentMap<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            m = new ConcurrentHashMap<>();
        }
        m.put(messageID, guid);
        LOG.info("storing inflight clientID <{}> messageID {} guid <{}>", clientID, messageID, guid);
        this.m_inflightStore.put(clientID, m);
    }

    @Override
    public BlockingQueue<IMessagesStore.StoredMessage> queue(String clientID) {
        LOG.info("Queuing pending message. ClientId={}, guid={}", clientID);
        return this.m_db.getQueue(clientID);
    }

    @Override
    public void dropQueue(String clientID) {
        LOG.info("Removing pending messages. ClientId={}", clientID);
        this.m_db.delete(clientID);
    }

    @Override
    public void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Moving inflight message to 2nd phase ack state. ClientId={}, messageID={}", clientID, messageID);
        }
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            LOG.warn("Unable to retrieve inflight message record. ClientId={}, messageId={}", clientID, messageID);
            return;
        }
        MessageGUID guid = m.remove(messageID);

        // remove from the ids store
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient != null) {
            inFlightForClient.remove(messageID);
        }

        final HashMap<Integer, MessageGUID> emptyGuids = new HashMap<>();
        Map<Integer, MessageGUID> messageIDs = Utils.defaultGet(m_secondPhaseStore, clientID, emptyGuids);
        messageIDs.put(messageID, guid);
        m_secondPhaseStore.put(clientID, messageIDs);
    }

    @Override
    public MessageGUID secondPhaseAcknowledged(String clientID, int messageID) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing second phase ACK CId={}, messageId={}", clientID, messageID);
        }
        final HashMap<Integer, MessageGUID> emptyGuids = new HashMap<>();
        Map<Integer, MessageGUID> messageIDs = Utils.defaultGet(m_secondPhaseStore, clientID, emptyGuids);
        MessageGUID guid = messageIDs.remove(messageID);
        m_secondPhaseStore.put(clientID, messageIDs);
        return guid;
    }

    @Override
    public IMessagesStore.StoredMessage getInflightMessage(String clientID, int messageID) {
        LOG.info("Retrieving inflight message CId={}, messageId={}", clientID, messageID);
        Map<Integer, MessageGUID> clientEntries = m_inflightStore.get(clientID);
        if (clientEntries == null) {
            LOG.warn("The client has no inflight messages CId={}, messageId={}", clientID, messageID);
            return null;
        }
        MessageGUID guid = clientEntries.get(messageID);
        if (guid == null) {
            LOG.warn("The message ID does not have an associated GUID. ClientId={}, messageId={}", clientID, messageID);
            return null;
        }
        return m_messagesStore.getMessageByGuid(guid);
    }

    @Override
    public int getInflightMessagesNo(String clientID) {
        if (!m_inflightStore.containsKey(clientID))
            return 0;
        else
            return m_inflightStore.get(clientID).size();
    }

    @Override
    public IMessagesStore.StoredMessage inboundInflight(String clientID, int messageID) {
        LOG.debug("Mapping inbound message ID to GUID CId={}, messageId={}", clientID, messageID);
        ConcurrentMap<Integer, MessageGUID> messageIdToGuid = m_db.getHashMap(inboundMessageId2GuidsMapName(clientID));
        final MessageGUID guid = messageIdToGuid.get(messageID);
        LOG.debug("Inbound message ID has been mapped CId={}, messageId={}, guid={}", clientID, messageID, guid);
        return m_messagesStore.getMessageByGuid(guid);
    }

    @Override
    public void markAsInboundInflight(String clientID, int messageID, MessageGUID guid) {
        ConcurrentMap<Integer, MessageGUID> messageIdToGuid = m_db.getHashMap(inboundMessageId2GuidsMapName(clientID));
        messageIdToGuid.put(messageID, guid);
    }

    @Override
    public int getPendingPublishMessagesNo(String clientID) {
        return queue(clientID).size();
    }

    @Override
    public int getSecondPhaseAckPendingMessages(String clientID) {
        if (!m_secondPhaseStore.containsKey(clientID))
            return 0;
        else
            return m_secondPhaseStore.get(clientID).size();
    }

    @Override
    public Collection<MessageGUID> pendingAck(String clientID) {
        ConcurrentMap<Integer, MessageGUID> messageGUIDMap = m_db.getHashMap(messageId2GuidsMapName(clientID));
        if (messageGUIDMap == null || messageGUIDMap.isEmpty()) {
            return Collections.emptyList();
        }

        return new ArrayList<>(messageGUIDMap.values());
    }

    static String messageId2GuidsMapName(String clientID) {
        return "guidsMapping_" + clientID;
    }

    static String inboundMessageId2GuidsMapName(String clientID) {
        return "inboundInflightGuidsMapping_" + clientID;
    }

    private ConcurrentMap<String, ConcurrentMap<Integer, MessageGUID>> getInFlightStore() {

    }

    private Map<String, Set<Integer>> getInFlightIds() {

    }

    private ConcurrentMap<String, PersistentSession> getPersistentSessions() {

    }

    private ConcurrentMap<String, Map<Integer, MessageGUID>> getSecondPhaseStore() {

    }
}
