package org.zenoss.zep;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.ResponseMeteredLevel;
import io.dropwizard.metrics.jetty12.AbstractInstrumentedHandler;

public class ZepInstrumentedHandler extends AbstractInstrumentedHandler {
    public ZepInstrumentedHandler(MetricRegistry registry) {
        super(registry);
    }

    public ZepInstrumentedHandler(MetricRegistry registry, String prefix) {
        super(registry, prefix);
    }

    public ZepInstrumentedHandler(MetricRegistry registry, String prefix, ResponseMeteredLevel responseMeteredLevel) {
        super(registry, prefix, responseMeteredLevel);
    }
}
