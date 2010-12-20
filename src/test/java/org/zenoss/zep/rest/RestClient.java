/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.zep.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.ProtobufConstants;

import com.google.protobuf.Message;

public class RestClient implements Closeable {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory
            .getLogger(RestClient.class);

    private HttpClient client = new DefaultHttpClient();
    private Map<String, Message> supportedMessages = new HashMap<String, Message>();
    private String host = System.getProperty("jetty.host", "localhost");
    private String port = System.getProperty("jetty.port", "8084");

    public static class RestResponse {
        private final int responseCode;
        private final Message message;

        public RestResponse(int responseCode, Message message) {
            this.responseCode = responseCode;
            this.message = message;
        }

        public int getResponseCode() {
            return responseCode;
        }

        public Message getMessage() {
            return message;
        }
    }

    public RestClient(Message... msgs) {
        for (Message msg : msgs) {
            this.supportedMessages.put(
                    msg.getDescriptorForType().getFullName(), msg);
        }
    }

    private URI createURI(String path) throws IOException {
        try {
            return new URI("http", null, host, Integer.valueOf(port), path,
                    null, null);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    private HttpEntity createProtobufEntity(Message msg) {
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContentType(ProtobufConstants.CONTENT_TYPE_PROTOBUF);
        entity.setContentLength(msg.getSerializedSize());
        entity.setContent(new ByteArrayInputStream(msg.toByteArray()));
        return entity;
    }

    private HttpEntity createJsonEntity(Message msg) throws IOException {
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContentType(MediaType.APPLICATION_JSON);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonFormat.writeTo(msg, baos);
        entity.setContent(new ByteArrayInputStream(baos.toByteArray()));
        return entity;
    }

    private Message createProtobufFromResponse(HttpResponse response)
            throws IllegalStateException, IOException {
        HttpEntity entity = response.getEntity();
        Message msg = null;
        Header fullNameHeader = response
                .getFirstHeader(ProtobufConstants.HEADER_PROTOBUF_FULLNAME);
        if (fullNameHeader != null) {
            Message msgForBuild = this.supportedMessages.get(fullNameHeader
                    .getValue());
            if (msgForBuild == null) {
                throw new IOException("Unsupported message type: "
                        + fullNameHeader.getValue());
            }
            String contentType = response.getFirstHeader("Content-Type")
                    .getValue();
            if (MediaType.APPLICATION_JSON.equals(contentType)) {
                msg = JsonFormat.merge(response.getEntity().getContent(),
                        msgForBuild.newBuilderForType());
            } else if (ProtobufConstants.CONTENT_TYPE_PROTOBUF
                    .equals(contentType)) {
                msg = (Message) msgForBuild.newBuilderForType()
                        .mergeFrom(response.getEntity().getContent()).build();
            } else {
                throw new IOException("Unsupported content type: "
                        + contentType);
            }
        }
        entity.consumeContent();
        return msg;

    }

    public RestResponse get(String path, String acceptContent)
            throws IOException {
        Message msg = null;
        HttpGet method = new HttpGet(createURI(path));
        method.addHeader("Accept", acceptContent);
        HttpResponse response = this.client.execute(method);
        int rc = response.getStatusLine().getStatusCode();
        if (rc == 200) {
            msg = createProtobufFromResponse(response);
        } else if (response.getStatusLine().getStatusCode() != 404) {
            throw new IOException("Unexpected RC: " + rc);
        }
        return new RestResponse(rc, msg);
    }

    public RestResponse getJson(String path) throws IOException {
        return get(path, MediaType.APPLICATION_JSON);
    }

    public RestResponse getProtobuf(String path) throws IOException {
        return get(path, ProtobufConstants.CONTENT_TYPE_PROTOBUF);
    }

    public RestResponse post(String path, Message msg, String contentType)
            throws IOException {
        HttpPost post = new HttpPost(createURI(path));
        post.addHeader(ProtobufConstants.HEADER_PROTOBUF_FULLNAME, msg
                .getDescriptorForType().getFullName());
        final HttpEntity entity;
        if (MediaType.APPLICATION_JSON.equals(contentType)) {
            entity = createJsonEntity(msg);
        } else if (ProtobufConstants.CONTENT_TYPE_PROTOBUF.equals(contentType)) {
            entity = createProtobufEntity(msg);
        } else {
            throw new IllegalArgumentException("Unsupported content type");
        }
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        Message msgRsp = null;
        if (response.getEntity() != null) {
            msgRsp = createProtobufFromResponse(response);
        }
        return new RestResponse(response.getStatusLine().getStatusCode(),
                msgRsp);
    }

    public RestResponse postJson(String path, Message msg) throws IOException {
        return post(path, msg, MediaType.APPLICATION_JSON);
    }

    public RestResponse postProtobuf(String path, Message msg)
            throws IOException {
        return post(path, msg, ProtobufConstants.CONTENT_TYPE_PROTOBUF);
    }

    public RestResponse put(String path, Message msg, String contentType)
            throws IOException {
        HttpPut put = new HttpPut(createURI(path));
        put.addHeader(ProtobufConstants.HEADER_PROTOBUF_FULLNAME, msg
                .getDescriptorForType().getFullName());
        final HttpEntity entity;
        if (MediaType.APPLICATION_JSON.equals(contentType)) {
            entity = createJsonEntity(msg);
        } else if (ProtobufConstants.CONTENT_TYPE_PROTOBUF.equals(contentType)) {
            entity = createProtobufEntity(msg);
        } else {
            throw new IllegalArgumentException("Unsupported content type");
        }
        put.setEntity(entity);
        HttpResponse response = client.execute(put);
        Message msgRsp = null;
        if (response.getEntity() != null) {
            msgRsp = createProtobufFromResponse(response);
        }
        return new RestResponse(response.getStatusLine().getStatusCode(),
                msgRsp);
    }

    public RestResponse putJson(String path, Message msg) throws IOException {
        return put(path, msg, MediaType.APPLICATION_JSON);
    }

    public RestResponse putProtobuf(String path, Message msg)
            throws IOException {
        return put(path, msg, ProtobufConstants.CONTENT_TYPE_PROTOBUF);
    }

    @Override
    public void close() throws IOException {
        this.client.getConnectionManager().shutdown();
    }
}
