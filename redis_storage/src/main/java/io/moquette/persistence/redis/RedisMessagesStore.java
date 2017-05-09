package io.moquette.persistence.redis;

import com.alibaba.fastjson.JSON;
import io.moquette.spi.IMatchingCondition;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.subscriptions.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.*;

/**
 * Created by ryan on 4/27/17.
 */
public class RedisMessagesStore implements IMessagesStore {
    private static final Logger LOG = LoggerFactory.getLogger(RedisMessagesStore.class);

    private JedisPool jedisPool;

    public RedisMessagesStore(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public void initStore() {
//        this.m_retainedStore = jedisPool.getResource().h
    }

    @Override
    public void storeRetained(Topic topic, MessageGUID guid) {
        LOG.debug("Storing retained messages. Topic={}, guid={}", topic, guid);

        Jedis jedis = jedisPool.getResource();
        try {
            jedis.hset(KeyConstants.RETAINED_STORE.getBytes(), JSON.toJSONBytes(topic), JSON.toJSONBytes(guid));
        } catch (Exception e) {
            LOG.error("Store retained messages is failed. ", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

    }

    @Override
    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
        LOG.debug("Scanning retained messages...");

        final List<StoredMessage> results = new ArrayList<>();
        Jedis jedis = jedisPool.getResource();

        try {
            Map<byte[], byte[]> storeMessages = jedis.hgetAll(KeyConstants.RETAINED_STORE.getBytes());

            storeMessages.entrySet().stream().forEach(it -> {
//                final MessageGUID messageGUID = JSON.parseObject(it.getValue(), MessageGUID.class);
                if (condition.match(JSON.parseObject(it.getKey(), Topic.class))) {
                    byte[] res = jedis.hget(KeyConstants.PERSISTENT_MSG_STORE.getBytes(), it.getValue());
                    if (res != null) {
                        results.add(JSON.parseObject(res, StoredMessage.class));
                    }
                }
            });

            if (LOG.isTraceEnabled()) {
                LOG.trace("The retained messages have been scanned. MatchingMessages={}", results);
            }


        } catch (Exception e) {
            LOG.error("Search match message is failed. ", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return results;
    }

    @Override
    public MessageGUID storePublishForFuture(StoredMessage storedMessage) {
        assert storedMessage.getClientID() != null : "Message to be persisted must have a valid client ID";
        MessageGUID guid = new MessageGUID(UUID.randomUUID().toString());
        storedMessage.setGuid(guid);
        LOG.debug("Storing publish event. CId={}, guid={}, topic={}", storedMessage.getClientID(), guid,
            storedMessage.getTopic());

        Jedis jedis = jedisPool.getResource();

        try {
            jedis.hset(KeyConstants.PERSISTENT_MSG_STORE.getBytes(), JSON.toJSONBytes(guid), JSON.toJSONBytes(storedMessage));
        } catch (Exception e) {
            LOG.error("Store publish is failed. ", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

        return guid;
    }

    @Override
    public void dropInFlightMessagesInSession(Collection<MessageGUID> pendingAckMessages) {
        //remove all guids from retained
        if (pendingAckMessages != null && pendingAckMessages.size() > 0) {
            final Jedis jedis = jedisPool.getResource();

            try {
                pendingAckMessages.stream().forEach(it -> {
                    final byte[] guidBytes = JSON.toJSONBytes(it);

                    Transaction tx = jedis.multi();
                    tx.hdel(KeyConstants.RETAINED_STORE.getBytes(), guidBytes);
                    tx.hdel(KeyConstants.PERSISTENT_MSG_STORE.getBytes(), guidBytes);
                    tx.exec();
                });
            } catch (Exception e) {
                LOG.error("Drop inflight messages is failed. ", e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }

    @Override
    public StoredMessage getMessageByGuid(MessageGUID guid) {
        LOG.debug("Retrieving stored message. Guid={}", guid);

        final Jedis jedis = jedisPool.getResource();

        try{
            byte[] res = jedis.hget(KeyConstants.PERSISTENT_MSG_STORE.getBytes(), JSON.toJSONBytes(guid));
            return res == null ? null : JSON.parseObject(res, StoredMessage.class);
        } catch (Exception e) {
            LOG.error("Clean retained messages is failed. ", e);
            return null;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public void cleanRetained(Topic topic) {
        LOG.debug("Cleaning retained messages. Topic={}", topic);
        final Jedis jedis = jedisPool.getResource();

        try{
            jedis.hdel(KeyConstants.RETAINED_STORE.getBytes(), JSON.toJSONBytes(topic));
        } catch (Exception e) {
            LOG.error("Clean retained messages is failed. ", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
