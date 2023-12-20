package org.zenoss.zep.logging;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Handler.Wrapper;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.slf4j.MDC;

import java.security.Principal;

public class ZepContextLogHandler extends Wrapper
{
    @Override
    public boolean handle(Request request, Response response, Callback callback) throws Exception
    {
        // Collect Info for NDC/MDC
        MDC.put("localname", request.getContext().getBaseResource().getName());
        MDC.put("servername", request.getConnectionMetaData().getServerAuthority().getHost());
        MDC.put("serverport", Integer.toString(request.getConnectionMetaData().getServerAuthority().getPort()));

        String contextPath = request.getContext().getContextPath();
        if (contextPath != null)
        {
            MDC.put("contextPath", contextPath);
        }
        MDC.put("remoteAddr", request.getConnectionMetaData().getRemoteSocketAddress().toString());

        Principal principal = Request.getAuthenticationState(request).getUserPrincipal();
        if (principal != null)
        {
            MDC.put("principal", principal.getName());
        }

        try
        {
            return super.handle(request, response, callback);
        }
        finally
        {
            // Pop info out / clear the NDC/MDC
            MDC.clear();
        }
    }
}
