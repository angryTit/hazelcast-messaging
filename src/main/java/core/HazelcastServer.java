package core;

import conf.HazelcastServerConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class HazelcastServer {
    public static void main(String[] args) {
        new AnnotationConfigApplicationContext(HazelcastServerConfig.class);
    }
}
