package biz.configuration;

import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.AdaptableJobFactory;

@Configuration
public class ScheduleConfiguration {

    @Autowired
    private AutowireCapableBeanFactory capableBeanFactory;

    @Bean
    public AdaptableJobFactory jobFactory() {
        return new AdaptableJobFactory() {
            @Override
            protected Object createJobInstance(TriggerFiredBundle bundle) throws Exception {
                Object instance = super.createJobInstance(bundle);
                capableBeanFactory.autowireBean(instance);
                return instance;
            }
        };
    }
}
