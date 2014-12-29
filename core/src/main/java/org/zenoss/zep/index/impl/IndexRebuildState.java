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
import org.zenoss.zep.dao.EventBatchParams;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

/**
* Saves state of index rebuild process so it can be restarted if ZEP is shut down.
*/
class IndexRebuildState {
    public final long began;              // when did this rebuild begin? (millis since the epoch)
    public final long updated;            // when was the rebuild state last updated? (milliseconds since the epoch)
    public final long indexed;            // count of events re-indexed so far
    public final Long expected;           // estimated total number of events being re-indexed
    public final int indexVersion;        // version number to correspond with code changes
    public final byte[] indexVersionHash; // checksum to correspond with configuration changes that affect the index
    public final long throughTime;        // ignore events with update_time after this cut-off (millis since the epoch)
    public final Long nextLastSeen;       // rebuild progress watermark, based on events' last_seen field
    public final String nextUuid;         // rebuild progress watermark, based on events' uuid field
    public final Long ended;              // when did this rebuild finish? (millis since the epoch)
    private final boolean estimatable;
    private final double percent;
    private final long eta;

    private static final String BEGAN          = "zep.index.progress.began_at";
    private static final String UPDATED        = "zep.index.progress.updated_at";
    private static final String ENDED          = "zep.index.progress.ended_at";
    private static final String INDEXED        = "zep.index.progress.indexed";
    private static final String EXPECTED       = "zep.index.progress.expected";
    private static final String INDEX_VERSION  = "zep.index.version";
    private static final String INDEX_HASH     = "zep.index.version_hash";
    private static final String THROUGH_TIME   = "zep.index.through_time";
    private static final String NEXT_LAST_SEEN = "zep.index.starting_last_seen";
    private static final String NEXT_UUID      = "zep.index.starting_uuid";

    private IndexRebuildState(long began, long updated, long indexed, Long expected,
                              int indexVersion, byte[] indexVersionhash, long throughTime,
                              Long nextLastSeen, String nextUuid, Long ended) {
        this.began = began;
        this.updated = updated;
        this.indexed = indexed;
        this.expected = expected;
        this.indexVersion = indexVersion;
        this.indexVersionHash = indexVersionhash;
        this.throughTime = throughTime;
        this.nextLastSeen = nextLastSeen;
        this.nextUuid = nextUuid;
        this.ended = ended;

        this.estimatable =
                ended != null || (expected != null && expected > 0 && indexed > 0 && began > 0 && updated > began);

        this.percent =
                (ended != null) ? 1 :
                (expected == null || expected <= 0 || indexed <= 0) ? 0 :
                (indexed > expected) ? 1 :
                (((double)indexed) / expected);

        this.eta =
                (ended != null) ? ended :
                estimatable ? (began + (long)((updated - began) / percent())) :
                0;
    }

    public static final IndexRebuildState UNKNOWN = begin(-1, null, -1);

    public static IndexRebuildState begin(int indexVersion, byte[] indexVersionHash, long throughTime) {
        long now = System.currentTimeMillis();
        return new IndexRebuildState(now, now, 0, null,
                indexVersion, indexVersionHash, throughTime,
                null, null, null);
    }

    public static IndexRebuildState update(IndexRebuildState state, long indexedDelta, Long nextLastSeen, String nextUuid) {

//        if (state.nextLastSeen != null) {
//            if (nextLastSeen == null || state.nextLastSeen < nextLastSeen) {
//                // state has already advanced beyond the partition, just use state.
//                nextLastSeen = state.nextLastSeen;
//                nextUuid = state.nextUuid;
//            } else if (state.nextLastSeen == nextLastSeen) {
//                // same partition
//                if (state.nextUuid != null && (nextUuid == null || state.nextUuid.compareTo(nextUuid) > 0)) {
//                   // state has already advanced beyond the uuid, just use state.
//                   nextUuid = state.nextUuid;
//                }
//            }
//        }
        long now = System.currentTimeMillis();
        return new IndexRebuildState(state.began, now, state.indexed + indexedDelta, state.expected,
                state.indexVersion, state.indexVersionHash, state.throughTime,
                nextLastSeen, nextUuid, state.ended);
    }

    public static IndexRebuildState end(IndexRebuildState state, long indexedDelta) {
        long now = System.currentTimeMillis();
        return new IndexRebuildState(state.began, now, state.indexed + indexedDelta, state.expected,
                state.indexVersion, state.indexVersionHash, state.throughTime,
                null, null, now);
    }

    public static IndexRebuildState expected(IndexRebuildState state, long expected) {
        return new IndexRebuildState(state.began, state.updated, state.indexed, expected,
                state.indexVersion, state.indexVersionHash, state.throughTime,
                state.nextLastSeen, state.nextUuid, state.ended);
    }

    private static IndexRebuildState fromProperties(Properties properties) throws ParseException {
        long began = ZepUtils.parseUTC(properties.getProperty(BEGAN)).getTime();
        long updated = ZepUtils.parseUTC(properties.getProperty(UPDATED)).getTime();
        String tmp = properties.getProperty(ENDED);
        Long ended = (tmp == null) ? null : ZepUtils.parseUTC(tmp).getTime();
        long indexed = Long.parseLong(properties.getProperty(INDEXED),10);
        tmp = properties.getProperty(EXPECTED);
        Long expected = (tmp == null) ? null : Long.parseLong(tmp, 10);
        int indexVersion = Integer.valueOf(properties.getProperty(INDEX_VERSION));
        tmp = properties.getProperty(INDEX_HASH);
        byte[] indexVersionHash = (tmp == null) ? null : ZepUtils.fromHex(tmp);
        long throughTime = ZepUtils.parseUTC(properties.getProperty(THROUGH_TIME)).getTime();
        tmp = properties.getProperty(NEXT_LAST_SEEN);
        Long nextLastSeen = (tmp == null) ? null : Long.parseLong(tmp, 10);
        String nextUuid = properties.getProperty(NEXT_UUID);
        return new IndexRebuildState(began, updated, indexed, expected,
                indexVersion, indexVersionHash, throughTime,
                nextLastSeen, nextUuid, ended);
    }

    private Properties toProperties() {
        Properties properties = new Properties();
        properties.setProperty(BEGAN, ZepUtils.formatUTC(began));
        properties.setProperty(UPDATED, ZepUtils.formatUTC(updated));
        if (ended != null)
            properties.setProperty(ENDED, ZepUtils.formatUTC(ended));
        properties.setProperty(INDEXED, Long.toString(indexed));
        if (expected != null)
            properties.setProperty(EXPECTED, expected.toString());
        properties.setProperty(INDEX_VERSION, Integer.toString(indexVersion));
        if (indexVersionHash != null)
            properties.setProperty(INDEX_HASH, ZepUtils.hexstr(indexVersionHash));
        properties.setProperty(THROUGH_TIME, ZepUtils.formatUTC(throughTime));
        if (nextLastSeen != null)
            properties.setProperty(NEXT_LAST_SEEN, nextLastSeen.toString());
        if (nextUuid != null)
            properties.setProperty(NEXT_UUID, nextUuid);
        return properties;
    }

    /**
     * Saves the current state to a file (to be able to restart the index rebuilding process).
     *
     * @param file The file to save the current state to.
     * @throws IOException If the current state cannot be saved.
     */
    public void save(File file) throws IOException {
        FileOutputStream fos = null;
        File tempFile = null;
        try {
            tempFile = File.createTempFile("tmp", ".properties", file.getParentFile());
            fos = new FileOutputStream(tempFile);
            toProperties().store(fos, "ZEP Internal Indexing State - Do Not Modify");
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
    public static IndexRebuildState loadState(File file) throws IOException, ParseException {
        IndexRebuildState state = null;
        if (file.isFile()) {
            BufferedInputStream bis = null;
            try {
                bis = new BufferedInputStream(new FileInputStream(file));
                Properties properties = new Properties();
                properties.load(bis);
                state = IndexRebuildState.fromProperties(properties);
            } catch (Exception e) {
                throw new IOException(e.getLocalizedMessage(), e);
            } finally {
                ZepUtils.close(bis);
            }
        }
        return state;
    }

    public EventBatchParams batchParams() {
        return new EventBatchParams(nextLastSeen, nextUuid);
    }

    public String toString() {
        return toProperties().toString();
    }

    public boolean isDone() {
        return ended != null;
    }

    /** Returns a value between 0 and 1. */
    public double percent() {
        return percent;
    }

    /** Estimated time of arrival (completion), in milliseconds since the epoch. */
    public long eta() {
        return eta;
    }

    public boolean hasEstimate() {
        return estimatable;
    }
}
