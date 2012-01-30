package org.zenoss.zep.impl;

import org.springframework.context.ApplicationListener;
import org.zenoss.protobufs.zep.Zep.ZepConfig;
import org.zenoss.zep.ZepConfigService;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.dao.ConfigDao;
import org.zenoss.zep.events.ZepConfigUpdatedEvent;


public class ZepConfigServiceImpl implements ZepConfigService,
        ApplicationListener<ZepConfigUpdatedEvent> {

    private ConfigDao configDao;
    private volatile ZepConfig config = null;

    public void setConfigDao(ConfigDao configDao) {
        this.configDao = configDao;
    }
    
    @Override
    public ZepConfig getConfig() throws ZepException {
        if (config == null) {
            config = configDao.getConfig();
        }
        return config;
    }

    @Override
    public void onApplicationEvent(ZepConfigUpdatedEvent event) {
        this.config = event.getConfig();
    }
}
