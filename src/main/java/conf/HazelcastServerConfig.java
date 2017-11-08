package conf;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static conf.Constants.EXECUTOR_NAME;

@Configuration
public class HazelcastServerConfig {
    @Bean
    public HazelcastInstance hazelcastInstance() {
        Config config = new Config();
        config.getScheduledExecutorConfig(EXECUTOR_NAME.name())
                .setPoolSize(4)
                .setCapacity(0)
                .setDurability(1);
        return Hazelcast.newHazelcastInstance(config);
    }
}
