package core;

import conf.HazelcastClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class HazelcastClientConsumer {
    private static final Logger log = LoggerFactory.getLogger(HazelcastClientProducer.class);

    public static void main(String[] args) {
        new AnnotationConfigApplicationContext(HazelcastClientConfig.class);
        log.info("Start listening");
    }
}
