package org.zenoss.zep;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.metrics.servlets.HealthCheckServlet;
import io.dropwizard.metrics.servlets.MetricsServlet;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.support.WebApplicationContextUtils;

@WebListener
public class ZepMetricsServletContextListener implements ServletContextListener {

    @Autowired
    private MetricRegistry metricRegistry;

    @Autowired
    private HealthCheckRegistry healthCheckRegistry;

    private final MetricsServletContextListener metricsServletContextListener = new MetricsServletContextListener();
    private final HealthCheckServletContextListener healthCheckServletContextListener = new HealthCheckServletContextListener();

    @Override
    public void contextInitialized(ServletContextEvent event) {
        WebApplicationContextUtils.getRequiredWebApplicationContext(event.getServletContext()).getAutowireCapableBeanFactory().autowireBean(this);

        metricsServletContextListener.contextInitialized(event);
        healthCheckServletContextListener.contextInitialized(event);
    }

    class MetricsServletContextListener extends MetricsServlet.ContextListener {

        @Override
        protected MetricRegistry getMetricRegistry() {
            return metricRegistry;
        }

    }

    class HealthCheckServletContextListener extends HealthCheckServlet.ContextListener {

        @Override
        protected HealthCheckRegistry getHealthCheckRegistry() {
            return healthCheckRegistry;
        }

    }

}
