package org.zenoss.zep.logging;

import org.eclipse.jetty.deploy.App;
import org.eclipse.jetty.deploy.AppLifeCycle;
import org.eclipse.jetty.deploy.graph.Node;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.ee10.webapp.WebAppContext;

public class ZepCentralizedWebAppLoggingBinding implements AppLifeCycle.Binding
{
    public String[] getBindingTargets()
    {
        return new String[]
                {"deploying"};
    }

    public void processBinding(Node node, App app) throws Exception
    {
        ContextHandler handler = app.getContextHandler();
        if (handler == null)
        {
            throw new NullPointerException("No Handler created for App: " + app);
        }

        if (handler instanceof WebAppContext)
        {
            WebAppContext webapp = (WebAppContext)handler;
            webapp.getSystemClassMatcher().add("org.apache.log4j.");
            webapp.getSystemClassMatcher().add("org.slf4j.");
            webapp.getSystemClassMatcher().add("org.apache.commons.logging.");

            webapp.getSystemClassMatcher().add("-org.apache.log4j.");
            webapp.getSystemClassMatcher().add("-org.slf4j.");
            webapp.getSystemClassMatcher().add("-org.apache.commons.logging.");
        }
    }
}
