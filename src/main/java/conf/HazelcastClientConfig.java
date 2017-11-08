package conf;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import core.DelayedMessageProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.hazelcast.inbound.HazelcastEventDrivenMessageProducer;
import org.springframework.integration.hazelcast.outbound.HazelcastCacheWritingMessageHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import util.HazelcastMessagingUtil;

import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

import static conf.Constants.EXECUTOR_NAME;
import static conf.Constants.HEADER_DELAY;

@Configuration
public class HazelcastClientConfig {

    @Bean
    public HazelcastInstance hazelcastInstance() {
        ClientConfig config = new ClientConfig();
        return HazelcastClient.newHazelcastClient(config);
    }

    @Bean
    public IScheduledExecutorService scheduledExecutorService(HazelcastInstance hazelcastInstance) {
        IScheduledExecutorService scheduledExecutorService =
                hazelcastInstance.getScheduledExecutorService(EXECUTOR_NAME.name());
        return scheduledExecutorService;
    }

    @Bean
    public IMap distributedNonDelayMessageMap(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap("DISTRIBUTED_NON_DELAY_MAP_NAME");
    }

    @Bean
    public IMap distributedInProgressMessageMap(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap("DISTRIBUTED_IN_PROGRESS_MAP_NAME");
    }

    @Bean
    public HazelcastCacheWritingMessageHandler writeNonDelayHandler(
            IMap distributedNonDelayMessageMap) {

        return HazelcastMessagingUtil.
                newHazelcastCacheWritingMessageHandler(distributedNonDelayMessageMap);
    }

    @Bean
    public HazelcastEventDrivenMessageProducer eventNonDelayProducer(
            IMap distributedNonDelayMessageMap,
            IMap distributedInProgressMessageMap,
            IScheduledExecutorService scheduledExecutorService,
            HazelcastInstance hazelcastInstance,
            MessageHandler simpleHandler) {

        return HazelcastMessagingUtil.newHazelcastEventDrivenMessageProducer(
                distributedNonDelayMessageMap,
                distributedInProgressMessageMap,
                scheduledExecutorService,
                hazelcastInstance,
                simpleHandler
        );
    }

    @Bean
    public MessageHandler simpleHandler() {
        return message -> System.out.println("Handle at " + LocalTime.now().toString() + ", message : " + message);
    }

    @Bean
    public MessageChannel channelWithoutDelay(
            HazelcastCacheWritingMessageHandler writeNonDelayMatchHandler) {
        PublishSubscribeChannel subscribeChannel = new PublishSubscribeChannel();
        subscribeChannel.subscribe(writeNonDelayMatchHandler);
        return subscribeChannel;
    }

    @Bean
    public MessageChannel inputChannel(
            IMap distributedNonDelayMessageMap,
            IScheduledExecutorService scheduledExecutorService,
            MessageChannel channelWithoutDelay) {
        PublishSubscribeChannel subscribeChannel = new PublishSubscribeChannel();
        subscribeChannel.subscribe(message -> {
            if (message.getHeaders().containsKey(HEADER_DELAY.name())) {
                DelayedMessageProcessor delayedMessageProcessor =
                        new DelayedMessageProcessor(distributedNonDelayMessageMap.getName(), message);
                long delay = (Long) message.getHeaders().get(HEADER_DELAY.name());
                scheduledExecutorService.schedule(delayedMessageProcessor, delay, TimeUnit.MILLISECONDS);
            } else {
                channelWithoutDelay.send(message);
            }
        });
        return subscribeChannel;
    }
}
