package org.zenoss.zep.logging.filter;


import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.AbstractMatcherFilter;
import ch.qos.logback.core.spi.FilterReply;

public class LoggerNameFilter extends AbstractMatcherFilter<ILoggingEvent> {
    private String loggerName;

    public void setLoggerName(String loggerName) {
        this.loggerName = loggerName;
    }

    @Override
    public FilterReply decide(ILoggingEvent event) {
        return event.getLoggerName().equals(loggerName) ? onMatch : onMismatch;
    }
}