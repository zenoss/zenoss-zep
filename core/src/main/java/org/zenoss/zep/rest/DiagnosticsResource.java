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

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.CacheControl;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import jakarta.ws.rs.core.StreamingOutput;
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
