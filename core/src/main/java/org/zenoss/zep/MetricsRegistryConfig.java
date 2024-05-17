package org.zenoss.zep;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
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

    private void skipOrRegister(MetricRegistry registry, String metricSetName, MetricSet metricSet) {
        if (registry.getNames().stream().noneMatch(n->n.startsWith(metricSetName))) {
            registry.register(metricSetName, metricSet);
            logger.info("Register {} metrics set in zep registry", metricSetName);
        } else {
            logger.warn("Metrics set {} already exist in zep registry", metricSetName);
        }
    }
    @Bean
    public GarbageCollectorMetricSet garbageCollectorMetricSet(@Autowired MetricRegistry registry) {
        GarbageCollectorMetricSet metricSet = new GarbageCollectorMetricSet();
        String metricSetName = "jvm.gc";
        skipOrRegister(registry, metricSetName, metricSet);
        return metricSet;
    }

    @Bean
    public MemoryUsageGaugeSet memoryUsageGaugeSet(@Autowired MetricRegistry registry) {
        MemoryUsageGaugeSet metricSet = new MemoryUsageGaugeSet();
        String metricSetName = "jvm.memory";
        skipOrRegister(registry, metricSetName, metricSet);
        return metricSet;
    }

    @Bean
    public ThreadStatesGaugeSet threadStatesGaugeSet(@Autowired MetricRegistry registry) {
        ThreadStatesGaugeSet metricSet = new ThreadStatesGaugeSet();
        String metricSetName = "jvm.thread-states";
        skipOrRegister(registry, metricSetName, metricSet);
        return metricSet;
    }

    @Bean
    public FileDescriptorRatioGauge fileDescriptorRatioGauge(@Autowired MetricRegistry registry) {
        FileDescriptorRatioGauge metric = new FileDescriptorRatioGauge();
        String metricName = "jvm.fd.usage";
        if (!registry.getNames().contains(metricName)) {
            registry.register(metricName, metric);
            logger.info("Register {} metric in zep registry", metricName);
        } else {
            logger.warn("Metric {} already exist in zep registry", metricName);
        }
        return metric;
    }
}
