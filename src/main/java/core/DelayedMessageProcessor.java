package core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.io.Serializable;
import java.time.LocalTime;

public class DelayedMessageProcessor implements Runnable, HazelcastInstanceAware, Serializable {
    private static final Logger log = LoggerFactory.getLogger(DelayedMessageProcessor.class);

    private transient HazelcastInstance hazelcastInstance;

    private final String targetMapName;
    private final Message message;

    public DelayedMessageProcessor(String targetMapName, Message message) {
        this.targetMapName = targetMapName;
        this.message = message;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void run() {
        log.info("Run at : {}, put message : {} into distributed map", LocalTime.now(), message);
        IMap targetMap = hazelcastInstance.getMap(targetMapName);
        String messageId = message.getHeaders().getId().toString();
        targetMap.put(messageId, message);
    }
}
