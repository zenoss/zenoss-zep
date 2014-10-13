/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.zep.rest;

import com.codahale.metrics.jvm.ThreadDump;

import java.lang.management.ThreadMXBean;
import java.lang.management.ManagementFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Diagnostics service.
 */
@Path("1.0/diagnostics")
public class DiagnosticsResource {

    private static final CacheControl NO_CACHE = new CacheControl();
    static {
        NO_CACHE.setNoCache(true);
        NO_CACHE.setNoStore(true);
        NO_CACHE.setMustRevalidate(true);
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("threads")
    public Response getThreadDump() {
        ResponseBuilder response = Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                new ThreadDump(threadMXBean).dump(output);
            }
        });
        response.cacheControl(NO_CACHE);
        return response.build();
    }
}
