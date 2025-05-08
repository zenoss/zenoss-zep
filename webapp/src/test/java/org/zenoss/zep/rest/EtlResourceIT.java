package org.zenoss.zep.rest;

import com.csvreader.CsvReader;
import com.google.protobuf.Message;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.zenoss.protobufs.zep.Zep.EventQuery;
import org.zenoss.zep.rest.RestClient.RestResponse;

import java.io.InputStreamReader;

import static junit.framework.Assert.*;

@ContextConfiguration({"classpath:zep-config.xml"})
public class EtlResourceIT extends AbstractJUnit4SpringContextTests {

    private RestClient client;

    @Before
    public void setUp() throws Exception {
        client = new RestClient(EventQuery.getDefaultInstance());
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        client = null;
    }

    @Test
    public void testGetEventEtlCsv() throws Exception {
        String path = "/zeneventserver/api/2/etl/search";
        Message msg = EventQuery.newBuilder().build();
        RestResponse resp = client.postJson(path, msg);
        assertEquals("Response code is not 200 OK.", HttpStatus.SC_OK, resp.getResponseCode());
        CsvReader csvReader = new CsvReader(new InputStreamReader(resp.getResponse().getEntity().getContent()));
        while (csvReader.readRecord()) {
            assertEquals("Unexpected column count.", 24, csvReader.getColumnCount());
            for (int i = 0; i < csvReader.getColumnCount(); i++) {
                csvReader.get(i);
            }
        }
        csvReader.close();
    }
}
