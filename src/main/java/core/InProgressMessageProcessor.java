package core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static java.util.Objects.nonNull;

public class InProgressMessageProcessor implements Runnable, HazelcastInstanceAware, Serializable {
    private static final Logger log = LoggerFactory.getLogger(InProgressMessageProcessor.class);

    private transient HazelcastInstance hazelcastInstance;

    private final String sourceMapName;
    private final String targetMapName;
    private final String messageId;

    public InProgressMessageProcessor(String sourceMapName, String targetMapName, String messageId) {
        this.sourceMapName = sourceMapName;
        this.targetMapName = targetMapName;
        this.messageId = messageId;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void run() {
        log.info("Replace message from source map to target map, messageId : {}", messageId);
        IMap sourceMap = hazelcastInstance.getMap(sourceMapName);
        IMap targetMap = hazelcastInstance.getMap(targetMapName);
        Object message = sourceMap.get(messageId);
        if (nonNull(message)) {
            targetMap.put(messageId, message);
            sourceMap.remove(messageId);
        }
    }
}
