/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.utils;

import java.net.InetAddress;

/**
 * Represents a range of IP addresses.
 */
public class IpRange {
    private final InetAddress from;
    private final InetAddress to;

    /**
     * Creates an IP range from the starting to ending addresses.
     * 
     * @param from Starting address.
     * @param to Ending address.
     */
    public IpRange(InetAddress from, InetAddress to) {
        if (from.getAddress().length != to.getAddress().length) {
            throw new IllegalArgumentException("Invalid range");
        }
        if (compareTo(from, to) > 0) {
            throw new IllegalArgumentException(from.getHostAddress() + " > " + to.getHostAddress());
        }
        this.from = from;
        this.to = to;
    }

    /**
     * Returns the starting address.
     * 
     * @return The starting address.
     */
    public InetAddress getFrom() {
        return this.from;
    }

    /**
     * Returns the ending address.
     * 
     * @return The ending address.
     */
    public InetAddress getTo() {
        return this.to;
    }

    /**
     * Returns true if the address specified is within the range.
     * 
     * @param addr The address to test.
     * @return True if the IP address is within the range of this IpRange.
     */
    public boolean contains(InetAddress addr) {
        return (compareTo(this.from, addr) <= 0 && compareTo(addr, this.to) <= 0);
    }
    
    private static int compareTo(InetAddress first, InetAddress second) {
        final byte[] addr1 = first.getAddress();
        final byte[] addr2 = second.getAddress();
        if (addr1.length != addr2.length) {
            throw new IllegalArgumentException("Invalid addresses");
        }
        for (int i = 0; i < addr1.length; i++) {
            final int octet1 = (addr1[i] & 0xff);
            final int octet2 = (addr2[i] & 0xff);
            if (octet1 != octet2) {
                return (octet1 > octet2) ? 1 : -1;
            }
        }
        return 0;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + this.from.hashCode();
        result = 37 * result + this.to.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IpRange)) {
            return false;
        }
        IpRange other = (IpRange) obj;
        return this.from.equals(other.from) && this.to.equals(other.to);
    }

    @Override
    public String toString() {
        return "IpRange{" +
                "from=" + from.getHostAddress() +
                ", to=" + to.getHostAddress() +
                '}';
    }
}
