/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.utils;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for IP address utilities.
 */
public class IpUtilsTest {
    @Test
    public void testParseAddress() throws UnknownHostException {
        List<String> validIps = Arrays.asList("192.168.1.2", "0.0.0.0", "255.255.255.0", "::1",
                "fe80::d69a:20ff:febe:2e22");
        for (String ip : validIps) {
            InetAddress parsed = IpUtils.parseAddress(ip);
            assertEquals(InetAddress.getByName(ip), parsed);
        }
        
        List<String> invalidIps = Arrays.asList("192.168.1.256", "192.168.-1.5", "www.zenoss.com", "ge80::");
        for (String ip : invalidIps) {
            try {
                IpUtils.parseAddress(ip);
                fail("Expected to fail with IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                // Expected
            }
        }
    }
    
    @Test
    public void testCanonicalIpAddress() throws UnknownHostException {
        final Map<String,String> expected = new HashMap<String,String>();
        expected.put("000.000.000.000", "0.0.0.0");
        expected.put("192.168.001.002", "192.168.1.2");
        expected.put("0000:0000:0000:0000:0000:0000:0000:0001", "::1");
        expected.put("fe80:0000:0000:0000:0000:0000:0000:0001", "fe80::1");
        expected.put("fd00:6587:52d7:0047:d69a:20ff:febe:2e22", "fd00:6587:52d7:47:d69a:20ff:febe:2e22");
        for (Map.Entry<String,String> entry : expected.entrySet()) {
            assertEquals(entry.getKey(), IpUtils.canonicalIpAddress(InetAddress.getByName(entry.getValue())));
        }
    }
    
    @Test
    public void testPrefixBitsToNetmask() throws UnknownHostException {
        final Map<Integer,String> ipv4Expected = new HashMap<Integer,String>();
        ipv4Expected.put(24, "255.255.255.0");
        ipv4Expected.put(32, "255.255.255.255");
        ipv4Expected.put(1, "128.0.0.0");
        ipv4Expected.put(0, "0.0.0.0");
        for (Map.Entry<Integer,String> entry : ipv4Expected.entrySet()) {
            assertEquals(InetAddress.getByName(entry.getValue()), IpUtils.prefixBitsToNetmask(entry.getKey(), false));
        }
        
        final Map<Integer,String> ipv6Expected = new HashMap<Integer,String>();
        ipv6Expected.put(0, "::");
        ipv6Expected.put(64, "ffff:ffff:ffff:ffff::");
        ipv6Expected.put(32, "ffff:ffff::");
        ipv6Expected.put(128, "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff");
        for (Map.Entry<Integer,String> entry : ipv6Expected.entrySet()) {
            assertEquals(InetAddress.getByName(entry.getValue()), IpUtils.prefixBitsToNetmask(entry.getKey(), true));
        }
        
        final List<Integer> expectedIPv4Failures = Arrays.asList(-1, 33);
        for (Integer expectedFailure : expectedIPv4Failures) {
            try {
                IpUtils.prefixBitsToNetmask(expectedFailure, false);
                fail("Expected failure");
            } catch (IllegalArgumentException e) {
                // Expected
            }
        }
        
        final List<Integer> expectedIPv6Failures = Arrays.asList(-1, 129);
        for (Integer expectedFailure : expectedIPv6Failures) {
            try {
                IpUtils.prefixBitsToNetmask(expectedFailure, false);
                fail("Expected failure");
            } catch (IllegalArgumentException e) {
                // Expected
            }
        }
    }
    
    @Test
    public void testNetmaskToPrefixBits() throws UnknownHostException {
        final Map<String,Integer> expected = new HashMap<String,Integer>();
        expected.put("255.255.255.255", 32);
        expected.put("255.255.255.0", 24);
        expected.put("128.0.0.0", 1);
        expected.put("0.0.0.0", 0);
        expected.put("::", 0);
        expected.put("ffff:ffff:ffff:ffff::", 64);
        expected.put("ffff:ffff::", 32);
        expected.put("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 128);
        for (Map.Entry<String,Integer> entry : expected.entrySet()) {
            assertEquals(entry.getValue().intValue(), IpUtils.netmaskToPrefixBits(InetAddress.getByName(entry.getKey())));
        }
        
        final List<String> expectedFailures = Arrays.asList("255.255.0.1", "ffff::1");
        for (String expectedFailure : expectedFailures) {
            try {
                IpUtils.netmaskToPrefixBits(InetAddress.getByName(expectedFailure));
                fail("Expected failure");
            } catch (IllegalArgumentException e) {
                // Expected
            }
        }
    }
    
    @Test
    public void testParseRange() throws UnknownHostException {
        final Map<String,IpRange> expected = new HashMap<String,IpRange>();
        expected.put("192.168.1.1-15", new IpRange(InetAddress.getByName("192.168.1.1"),
                InetAddress.getByName("192.168.1.15")));
        expected.put("192.168.1.1/24", new IpRange(InetAddress.getByName("192.168.1.0"),
                InetAddress.getByName("192.168.1.255")));
        expected.put("192.168.0.100/28", new IpRange(InetAddress.getByName("192.168.0.96"),
                InetAddress.getByName("192.168.0.111")));
        expected.put("192.168.1.1/255.255.255.0", new IpRange(InetAddress.getByName("192.168.1.0"),
                InetAddress.getByName("192.168.1.255")));
        expected.put("192.168.1.1-192.168.1.5", new IpRange(InetAddress.getByName("192.168.1.1"),
                InetAddress.getByName("192.168.1.5")));
        expected.put("192.168.1.1", new IpRange(InetAddress.getByName("192.168.1.1"),
                InetAddress.getByName("192.168.1.1")));
        expected.put("::1-::feed", new IpRange(InetAddress.getByName("::1"),
                InetAddress.getByName("::feed")));
        expected.put("::1-ffff", new IpRange(InetAddress.getByName("::1"),
                InetAddress.getByName("::ffff")));
        expected.put("fe80::d69a:20ff:febe:2e22/64", new IpRange(InetAddress.getByName("fe80::"),
                InetAddress.getByName("fe80::ffff:ffff:ffff:ffff")));
        for (Map.Entry<String,IpRange> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), IpUtils.parseRange(entry.getKey()));
        }
        
        List<String> expectedFailures = Arrays.asList("192.168.1.2-192.168.1.1", "::2-::1", "192.168.1.2-fe80::");
        for (String expectedFailure : expectedFailures) {
            try {
                IpUtils.parseRange(expectedFailure);
                fail("Expected to fail: " + expectedFailure);
            } catch (IllegalArgumentException e) {
                // Expected
            }
        }
    }
    
    @Test
    public void testIpRange() throws UnknownHostException {
        IpRange range = new IpRange(InetAddress.getByName("192.168.1.99"), InetAddress.getByName("192.168.1.150"));
        assertTrue(range.contains(InetAddress.getByName("192.168.1.99")));
        assertTrue(range.contains(InetAddress.getByName("192.168.1.120")));
        assertTrue(range.contains(InetAddress.getByName("192.168.1.150")));
        assertFalse(range.contains(InetAddress.getByName("192.168.1.98")));
        assertFalse(range.contains(InetAddress.getByName("192.168.1.151")));
        
        range = new IpRange(InetAddress.getByName("fe80::1"), InetAddress.getByName("fe80::7"));
        assertTrue(range.contains(InetAddress.getByName("fe80::1")));
        assertTrue(range.contains(InetAddress.getByName("fe80::6")));
        assertTrue(range.contains(InetAddress.getByName("fe80::7")));
        assertFalse(range.contains(InetAddress.getByName("fe80::")));
        assertFalse(range.contains(InetAddress.getByName("fe80::8")));
    }
}
