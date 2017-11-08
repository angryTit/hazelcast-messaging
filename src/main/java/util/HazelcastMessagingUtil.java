package util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import core.NonDelayMessageHandler;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.hazelcast.CacheListeningPolicyType;
import org.springframework.integration.hazelcast.inbound.HazelcastEventDrivenMessageProducer;
import org.springframework.integration.hazelcast.outbound.HazelcastCacheWritingMessageHandler;
import org.springframework.messaging.MessageHandler;

public class HazelcastMessagingUtil {

    public static HazelcastCacheWritingMessageHandler newHazelcastCacheWritingMessageHandler(IMap distributedMap) {

        HazelcastCacheWritingMessageHandler hazelcastCacheWritingMessageHandler =
                new HazelcastCacheWritingMessageHandler();
        hazelcastCacheWritingMessageHandler.setDistributedObject(distributedMap);
        hazelcastCacheWritingMessageHandler.setExtractPayload(false);

        ExpressionParser parser = new SpelExpressionParser();
        Expression exp = parser.parseExpression("headers.id.toString()");
        hazelcastCacheWritingMessageHandler.setKeyExpression(exp);

        return hazelcastCacheWritingMessageHandler;
    }

    public static HazelcastEventDrivenMessageProducer newHazelcastEventDrivenMessageProducer(
            IMap nonDelayMap,
            IMap inProgressMap,
            IScheduledExecutorService scheduledExecutorService,
            HazelcastInstance hazelcastInstance,
            MessageHandler handler) {

        HazelcastEventDrivenMessageProducer producer =
                new HazelcastEventDrivenMessageProducer(nonDelayMap);

        PublishSubscribeChannel innerChannel = new PublishSubscribeChannel();
        innerChannel.subscribe(handler);
        NonDelayMessageHandler innerHandler =
                new NonDelayMessageHandler(
                        nonDelayMap,
                        inProgressMap,
                        scheduledExecutorService,
                        hazelcastInstance, innerChannel);

        PublishSubscribeChannel receiverChannel = new PublishSubscribeChannel();
        receiverChannel.subscribe(innerHandler);

        producer.setOutputChannel(receiverChannel);
        producer.setCacheEventTypes("ADDED");
        producer.setCacheListeningPolicy(CacheListeningPolicyType.SINGLE);
        return producer;
    }
}
