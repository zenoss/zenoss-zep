/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.zep.index.impl;

import org.zenoss.zep.ZepUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

/**
* Saves state of index rebuild process so it can be restarted if ZEP is shut down.
*/
class IndexRebuildState {
    private final int indexVersion;
    private final byte[] indexVersionHash;
    private final long throughTime;
    private String startingUuid;

    public IndexRebuildState(int indexVersion, byte[] indexVersionhash, long throughTime, String startingUuid) {
        this.indexVersion = indexVersion;
        this.indexVersionHash = indexVersionhash;
        this.throughTime = throughTime;
        this.startingUuid = startingUuid;
    }

    public int getIndexVersion() {
        return indexVersion;
    }

    public byte[] getIndexVersionHash() {
        return indexVersionHash;
    }

    public long getThroughTime() {
        return throughTime;
    }

    public String getStartingUuid() {
        return startingUuid;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            int i = b & 0xff;
            String s = Integer.toHexString(i);
            if (s.length() == 1) {
                sb.append('0');
            }
            sb.append(s);
        }
        return sb.toString();
    }

    private static byte[] fromHex(String hexString) {
        if (hexString.length() % 2 != 0) {
            throw new IllegalArgumentException("Hex string should be an even number of characters");
        }
        byte[] bytes = new byte[hexString.length()/2];
        for (int i = 0; i < bytes.length; i++) {
            int strIndex = i * 2;
            bytes[i] = (byte) Integer.parseInt(hexString.substring(strIndex, strIndex+2), 16);
        }
        return bytes;
    }

    public void setStartingUuid(String startingUuid) {
        this.startingUuid = startingUuid;
    }

    /**
     * Saves the current state to a file (to be able to restart the index rebuilding process).
     *
     * @param file The file to save the current state to.
     * @throws IOException If the current state cannot be saved.
     */
    public void save(File file) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("zep.index.version", String.valueOf(indexVersion));
        if (indexVersionHash != null) {
            properties.setProperty("zep.index.version_hash", toHex(indexVersionHash));
        }
        properties.setProperty("zep.index.through_time", Long.toString(throughTime));
        if (startingUuid != null) {
            properties.setProperty("zep.index.starting_uuid", startingUuid);
        }

        BufferedWriter bw = null;
        File tempFile = null;
        try {
            tempFile = File.createTempFile("tmp", ".properties", file.getParentFile());
            bw = new BufferedWriter(new FileWriter(tempFile));
            properties.store(bw, "ZEP Internal Indexing State - Do Not Modify");
            bw.close();
            bw = null;
            if (!tempFile.renameTo(file)) {
                throw new IOException("Failed to rename " + tempFile.getAbsolutePath() + " to " +
                        file.getAbsolutePath());
            }
        } finally {
            ZepUtils.close(bw);
            if (tempFile != null && tempFile.isFile()) {
                if (!tempFile.delete()) {
                    tempFile.deleteOnExit();
                }
            }
        }
    }

    /**
     * Loads the state of the index rebuilding process from the specified file.
     *
     * @param file The file to load the state from.
     * @return The state loaded from the file, or null if the file doesn't exist or the state couldn't be read.
     * @throws IOException If an error occurs reading from the file.
     */
    public static IndexRebuildState loadState(File file) throws IOException {
        IndexRebuildState state = null;
        if (file.isFile()) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(file));
                Properties properties = new Properties();
                properties.load(br);

                int indexVersion = Integer.valueOf(properties.getProperty("zep.index.version"));
                byte[] indexVersionHash = null;
                String indexVersionHashStr = properties.getProperty("zep.index.version_hash");
                if (indexVersionHashStr != null) {
                    indexVersionHash = fromHex(indexVersionHashStr);
                }
                long throughTime = Long.valueOf(properties.getProperty("zep.index.through_time"));
                String startingUuid = properties.getProperty("zep.index.starting_uuid");

                state = new IndexRebuildState(indexVersion, indexVersionHash, throughTime, startingUuid);
            } catch (Exception e) {
                throw new IOException(e.getLocalizedMessage(), e);
            } finally {
                ZepUtils.close(br);
            }
        }
        return state;
    }
}
