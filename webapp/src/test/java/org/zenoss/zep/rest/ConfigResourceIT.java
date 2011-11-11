/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.rest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zenoss.protobufs.zep.Zep.EventSeverity;
import org.zenoss.protobufs.zep.Zep.ZepConfig;
import org.zenoss.zep.rest.RestClient.RestResponse;

import java.io.IOException;
import java.net.HttpURLConnection;

import static org.junit.Assert.*;

/**
 * Test cases for Config REST API.
 */
public class ConfigResourceIT {
    private static final String CONFIG_URI = "/zeneventserver/api/1.0/config";

    private RestClient client;

    @Before
    public void setup() {
        this.client = new RestClient(ZepConfig.getDefaultInstance());
    }

    @After
    public void shutdown() throws IOException {
        this.client.close();
    }

    @Test
    public void testDeleteConfig() throws IOException {
        // Delete any current configuration item
        String configName = "event_age_disable_severity";
        RestResponse response = this.client.delete(CONFIG_URI + "/" + configName);

        ZepConfig config = ZepConfig.newBuilder().setEventAgeDisableSeverity(EventSeverity.SEVERITY_CRITICAL)
                .build();
        response = this.client.putProtobuf(CONFIG_URI + "/" + configName, config);
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.getResponseCode());

        response = this.client.getProtobuf(CONFIG_URI);
        assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
        ZepConfig configFromRest = (ZepConfig) response.getMessage();
        assertEquals(configFromRest.getEventAgeDisableSeverity(), config.getEventAgeDisableSeverity());

        // Delete the configuration item
        response = this.client.delete(CONFIG_URI + "/" + configName);
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.getResponseCode());

        // Deleting again should return a 404 not found
        response = this.client.delete(CONFIG_URI + "/" + configName);
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.getResponseCode());
    }
}
