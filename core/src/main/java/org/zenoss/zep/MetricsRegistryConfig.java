package org.zenoss.zep;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class MetricsRegistryConfig {

    private static final Logger logger = LoggerFactory.getLogger(MetricsRegistryConfig.class);
    @Bean
    public GarbageCollectorMetricSet garbageCollectorMetricSet(@Autowired MetricRegistry registry) {
        GarbageCollectorMetricSet metricSet = new GarbageCollectorMetricSet();
        String metricName = "jvm.gc";
        registry.register(metricName, metricSet);
        logger.info("Register {} metric in zep registry", metricName);
        return metricSet;
    }

    @Bean
    public MemoryUsageGaugeSet memoryUsageGaugeSet(@Autowired MetricRegistry registry) {
        MemoryUsageGaugeSet metricSet = new MemoryUsageGaugeSet();
        String metricName = "jvm.memory";
        registry.register(metricName, metricSet);
        logger.info("Register {} metric in zep registry", metricName);
        return metricSet;
    }

    @Bean
    public ThreadStatesGaugeSet threadStatesGaugeSet(@Autowired MetricRegistry registry) {
        ThreadStatesGaugeSet metricSet = new ThreadStatesGaugeSet();
        String metricName = "jvm.thread-states";
        registry.register(metricName, metricSet);
        logger.info("Register {} metric in zep registry", metricName);
        return metricSet;
    }

    @Bean
    public FileDescriptorRatioGauge fileDescriptorRatioGauge(@Autowired MetricRegistry registry) {
        FileDescriptorRatioGauge metric = new FileDescriptorRatioGauge();
        String metricName = "jvm.fd.usage";
        registry.register(metricName, metric);
        logger.info("Register {} metric in zep registry", metricName);
        return metric;
    }
}
