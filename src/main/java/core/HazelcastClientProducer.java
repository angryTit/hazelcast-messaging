package core;

import conf.HazelcastClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import java.time.LocalTime;
import java.util.HashMap;

import static conf.Constants.HEADER_DELAY;

public class HazelcastClientProducer {
    private static final Logger log = LoggerFactory.getLogger(HazelcastClientProducer.class);

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext app = new AnnotationConfigApplicationContext(HazelcastClientConfig.class);
        MessageChannel channel = app.getBean("inputChannel", MessageChannel.class);

        HashMap<String, Object> headers_1 = new HashMap<>();
        headers_1.put(HEADER_DELAY.name(), 1000L);
        Message message_1 = new GenericMessage<String>("Hello with 1000 delay", headers_1);

        HashMap<String, Object> headers_2 = new HashMap<>();
        headers_2.put(HEADER_DELAY.name(), 5000L);
        Message message_2 = new GenericMessage<String>("Hello with 5000 delay", headers_2);

        HashMap<String, Object> headers_3 = new HashMap<>();
        headers_3.put(HEADER_DELAY.name(), 14000L);
        Message message_3 = new GenericMessage<String>("Hello with 14000 delay", headers_3);

        Message message = new GenericMessage<String>("Hello with NO delay");


        channel.send(message_1);
        channel.send(message_2);
        channel.send(message_3);
        channel.send(message);

        log.info("Send messages at : {}", LocalTime.now());
    }
}
