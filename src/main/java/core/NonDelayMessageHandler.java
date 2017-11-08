package core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.hazelcast.message.EntryEventMessagePayload;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static java.util.Objects.nonNull;

public class NonDelayMessageHandler implements MessageHandler {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IMap distributedNonDelayMessageMap;
    private final IMap distributedInProgressMessageStore;
    private final IScheduledExecutorService scheduledExecutorService;
    private final HazelcastInstance hazelcastInstance;
    private final PublishSubscribeChannel outboundChannel;

    public NonDelayMessageHandler(IMap distributedNonDelayMessageMap,
                                  IMap distributedInProgressMessageStore,
                                  IScheduledExecutorService scheduledExecutorService,
                                  HazelcastInstance hazelcastInstance,
                                  PublishSubscribeChannel outboundChannel) {
        this.distributedNonDelayMessageMap = distributedNonDelayMessageMap;
        this.distributedInProgressMessageStore = distributedInProgressMessageStore;
        this.scheduledExecutorService = scheduledExecutorService;
        this.hazelcastInstance = hazelcastInstance;
        this.outboundChannel = outboundChannel;
    }

    @Override
    public void handleMessage(Message message) throws MessagingException {
        EntryEventMessagePayload entryEventMessagePayload = (EntryEventMessagePayload) message.getPayload();
        final String messageId = (String) entryEventMessagePayload.key;
        log.info("Handle event with key : {}", messageId);
        Lock lock = hazelcastInstance.getLock(messageId);
        Message originMessage = null;
        try {
            if (lock.tryLock(10, TimeUnit.SECONDS)) {
                log.info("Got a lock by key : {}", messageId);
                originMessage = (Message) distributedNonDelayMessageMap.get(messageId);
                if (nonNull(originMessage)) {
                    distributedInProgressMessageStore.put(messageId, originMessage);
                    scheduledExecutorService.schedule(
                            new InProgressMessageProcessor(
                                    distributedInProgressMessageStore.getName(),
                                    distributedNonDelayMessageMap.getName(),
                                    messageId),
                            10,
                            TimeUnit.MINUTES
                    );
                    log.info("Message is not null, put it into distributedInProgressMessageStore and setup core.InProgressMessageProcessor, key : {}", messageId);
                    distributedNonDelayMessageMap.remove(messageId);
                    log.info("Remove message from distributedNonDelayMessageMap, key : {}", messageId);

                }
            } else {
                log.warn("Did not get a lock during 10 sec by key : {}", messageId);
            }
        } catch (InterruptedException e) {
            log.error("Error while working with lock by key : " + messageId, e);
        } finally {
            lock.unlock();
            log.info("Release lock, key : {}", messageId);
        }
        if (nonNull(originMessage)) {
            log.info("Message is not null, process it. key : {}", messageId);
            processMessage(originMessage);
            distributedInProgressMessageStore.remove(messageId);
            log.info("Remove message from distributedInProgressMessageStore, key : {}", messageId);
        } else {
            log.debug("Message is null. messageId : {}", messageId);
        }
    }

    private void processMessage(Message message) {
        outboundChannel.send(message);
    }
}
