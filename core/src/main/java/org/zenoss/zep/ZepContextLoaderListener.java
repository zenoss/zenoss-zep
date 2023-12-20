/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.zep;

import org.jboss.resteasy.plugins.spring.SpringContextLoaderListener;

import jakarta.servlet.ServletContextEvent;
import java.lang.Runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.InterruptedException;


public class ZepContextLoaderListener extends SpringContextLoaderListener {

    private static final Logger logger = LoggerFactory.getLogger(ZepContextLoaderListener.class);

    @Override
    public void contextInitialized(ServletContextEvent event) {

        logger.debug("calling supercontextInitialized");
        try {
            super.contextInitialized(event);
            logger.debug("called supercontextInitialized");
        } catch (Throwable T) {
            logger.error("Could not initialize context, shutting down");
            if (logger.isDebugEnabled()) {
                T.printStackTrace();
            }
            try {
                // get our pid, send SIGKILL
                Process p = Runtime.getRuntime().exec(new String[]{"bash", "-c", "zeneventserver status | awk -F= '{ print $2 }' | xargs kill -9"});
                p.waitFor();
            } catch (IOException ex) {
            } catch (InterruptedException e) {
            }
        }
    }
}


