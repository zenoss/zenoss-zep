/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.index.impl;

import org.zenoss.zep.ZepUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
* Saves state of index rebuild process so it can be restarted if ZEP is shut down.
*/
class IndexRebuildState {
    private final int indexVersion;
    private final byte[] indexVersionHash;
    private final long throughTime;
    private Long startingLastSeen;
    private String startingUuid;

    public IndexRebuildState(int indexVersion, byte[] indexVersionhash, long throughTime, Long startingLastSeen, String startingUuid) {
        this.indexVersion = indexVersion;
        this.indexVersionHash = indexVersionhash;
        this.throughTime = throughTime;
        this.startingLastSeen = startingLastSeen;
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

    public Long getStartingLastSeen() {
        return startingLastSeen;
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

    public void setStartingLastSeen(Long startingLastSeen) {
        this.startingLastSeen = startingLastSeen;
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
        if (startingLastSeen != null) {
            properties.setProperty("zep.index.starting_last_seen", startingLastSeen.toString());
        }
        if (startingUuid != null) {
            properties.setProperty("zep.index.starting_uuid", startingUuid);
        }

        FileOutputStream fos = null;
        File tempFile = null;
        try {
            tempFile = File.createTempFile("tmp", ".properties", file.getParentFile());
            fos = new FileOutputStream(tempFile);
            properties.store(fos, "ZEP Internal Indexing State - Do Not Modify");
            fos.close();
            fos = null;
            if (!tempFile.renameTo(file)) {
                throw new IOException("Failed to rename " + tempFile.getAbsolutePath() + " to " +
                        file.getAbsolutePath());
            }
        } finally {
            ZepUtils.close(fos);
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
            BufferedInputStream bis = null;
            try {
                bis = new BufferedInputStream(new FileInputStream(file));
                Properties properties = new Properties();
                properties.load(bis);

                int indexVersion = Integer.valueOf(properties.getProperty("zep.index.version"));
                byte[] indexVersionHash = null;
                String indexVersionHashStr = properties.getProperty("zep.index.version_hash");
                if (indexVersionHashStr != null) {
                    indexVersionHash = fromHex(indexVersionHashStr);
                }
                long throughTime = Long.valueOf(properties.getProperty("zep.index.through_time"));
                String startingUuid = properties.getProperty("zep.index.starting_uuid");

                Long startingLastSeen = null;
                String startingLastSeenStr = properties.getProperty("zep.index.starting_last_seen");
                if (startingLastSeenStr != null) {
                    startingLastSeen = Long.valueOf(startingLastSeenStr);
                }

                state = new IndexRebuildState(indexVersion, indexVersionHash, throughTime, startingLastSeen, startingUuid);
            } catch (Exception e) {
                throw new IOException(e.getLocalizedMessage(), e);
            } finally {
                ZepUtils.close(bis);
            }
        }
        return state;
    }
}
