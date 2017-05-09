package io.moquette.persistence.redis;

import io.moquette.server.config.IConfig;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.IStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by ryan on 4/27/17.
 */
public class RedisPersistentStore implements IStore {
    private static final Logger LOG = LoggerFactory.getLogger(RedisPersistentStore.class);

    private JedisPool jedisPool;

    private IMessagesStore m_messageStore;
    private ISessionsStore m_sessionsStore;

    public RedisPersistentStore(IConfig props) {
        JedisPoolConfig configs = new JedisPoolConfig();

        this.jedisPool = new JedisPool(configs,
            props.getProperty("host", "localhost"),
            Integer.parseInt(props.getProperty("port", "6379")),
            Integer.parseInt(props.getProperty("timeout", "500")),
            props.getProperty("password"));
    }

    @Override
    public IMessagesStore messagesStore() {
        return this.m_messageStore;
    }

    public void initStore() {
        this.m_messageStore = new RedisMessagesStore(jedisPool);
        this.m_messageStore.initStore();

        this.m_sessionsStore = new RedisSessionStore(jedisPool);
        this.m_sessionsStore.initStore();
    }

    @Override
    public ISessionsStore sessionsStore() {
        return this.m_sessionsStore;
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
