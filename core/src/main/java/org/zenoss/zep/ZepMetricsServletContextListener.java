package org.zenoss.zep;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.metrics.servlets.MetricsServlet;

public class ZepMetricsServletContextListener extends MetricsServlet.ContextListener {
    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    @Override
    protected MetricRegistry getMetricRegistry() {
        return METRIC_REGISTRY;
    }
}
