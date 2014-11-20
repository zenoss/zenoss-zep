package org.zenoss.zep.index.impl;

import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.python.google.common.base.Strings;
import org.zenoss.zep.ZepException;
import org.zenoss.zep.index.EventIndexBackend;
import org.zenoss.zep.index.impl.MultiBackendEventIndexDao.BackendStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EventIndexBackendConfiguration implements Cloneable {

    private final String name;
    private EventIndexBackend backend;
    private volatile BackendStatus status;
    private volatile Boolean asyncUpdates;
    private volatile Boolean honorDeletes;
    private volatile Integer batchSize;
    private volatile Long lastCleared;
    private volatile Long lastRebuilt;

    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static EventIndexBackendConfiguration createInstance(boolean enabled, String name, EventIndexBackend backend,
                                                              BackendStatus status, boolean asyncUpdates,
                                                              boolean honorDeletes, int batchSize) throws Exception {

        if (!enabled){
            return null;
        }
        if (Strings.isNullOrEmpty(name)){
            throw new RuntimeException("EventIndexBackendConfiguration name must be specified");
        }
        if (backend == null){
            throw new RuntimeException("EventIndexBackend must not be null");
        }

        EventIndexBackendConfiguration result = new EventIndexBackendConfiguration(name, backend);
        result.setStatus(status);
        result.setAsyncUpdates(asyncUpdates);
        result.setHonorDeletes(honorDeletes);
        result.setBatchSize(batchSize);
        return result;
    }

    private EventIndexBackendConfiguration(String name, EventIndexBackend backend) {
        this.name = name;
        this.backend = backend;
    }

    /** You can only set the backend once, to a non-null value.
     *
     * @throws ZepException if the backend has already been set.
     */
    public synchronized void setBackend(EventIndexBackend backend) throws ZepException {
        if (this.backend == backend) return;
        if (this.backend == null)
            this.backend = backend;
        else
            throw new ZepException("Cannot modify the backend of " + name + " after it has already been set.");
    }

    public void setStatus(BackendStatus status) {
        this.status = status;
    }

    public void setAsyncUpdates(boolean asyncUpdates) {
        this.asyncUpdates = asyncUpdates;
    }

    public void setHonorDeletes(boolean honorDeletes) {
        this.honorDeletes = honorDeletes;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setLastRebuilt(long lastRebuilt) {
        this.lastRebuilt = lastRebuilt;
    }

    public void setLastCleared(long lastCleared) {
        this.lastCleared = lastCleared;
    }

    public String getName() {
        return name;
    }

    public EventIndexBackend getBackend() {
        return backend;
    }

    public BackendStatus getStatus() {
        return status == null ? BackendStatus.REGISTERED : status;
    }

    public boolean isWriter() {
        return status == BackendStatus.READER || status == BackendStatus.WRITER;
    }

    public boolean isAsyncUpdates() {
        return asyncUpdates == null || asyncUpdates;
    }

    public boolean isHonorDeletes() {
        return honorDeletes == null || honorDeletes;
    }

    public int getBatchSize() {
        return batchSize == null ? DEFAULT_BATCH_SIZE : batchSize;
    }

    public Long getLastRebuilt() {
        return lastRebuilt;
    }

    public Long getLastCleared() {
        return lastCleared;
    }

    public EventIndexBackendConfiguration clone() {
        try {
            return (EventIndexBackendConfiguration) super.clone();
        } catch (CloneNotSupportedException e) {
            // Should be impossible.
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("EventIndexBackendConfiguration[");
        ObjectMapper mapper = new ObjectMapper();
        Map<String,String> map = Maps.newHashMap();
        if (name != null) map.put("name", name);
        if (status != null) map.put("status", status.toString());
        if (asyncUpdates != null) map.put("asyncUpdates", asyncUpdates.toString());
        if (honorDeletes != null) map.put("honorDeletes", honorDeletes.toString());
        if (lastRebuilt != null) map.put("lastRebuilt", lastRebuilt.toString());
        if (lastCleared != null) map.put("lastCleared", lastCleared.toString());
        if (batchSize != null) map.put("batchSize", batchSize.toString());
        try {
            sb.append(mapper.writeValueAsString(map));
        } catch (IOException e) {
            throw new RuntimeException("Impossible exception: ", e);
        }
        sb.append("]");
        return sb.toString();
    }

    public static final Pattern PARSER = Pattern.compile(
            "\\A" + Pattern.quote("EventIndexBackendConfiguration[") + "(.*)" + Pattern.quote("]") + "\\z"
    );

    /** Parses the output of {@link #toString()}
     *
     * @return a configuration object (with null backend), or else null if the string was unparsable.
     */
    public static EventIndexBackendConfiguration parse(String s) {
        final Matcher matcher = PARSER.matcher(s);
        if (matcher.matches()) {
            Map<String,String> map;
            try {
                ObjectMapper mapper = new ObjectMapper();
                map = mapper.readValue(matcher.group(1).getBytes(), new TypeReference<HashMap<String,String>>() {});
            } catch (IOException e) {
                return null;
            }

            if (map.get("name") == null) return null;
            EventIndexBackendConfiguration result = new EventIndexBackendConfiguration(map.get("name"), null);

            if (map.get("status") != null) {
                try {
                    result.status = BackendStatus.valueOf(map.get("status"));
                } catch (IllegalArgumentException e) {
                    // ignore it
                }
            }

            if ("true".equals(map.get("asyncUpdates")))
                result.asyncUpdates = true;
            else if ("false".equals(map.get("asyncUpdates")))
                result.asyncUpdates = false;

            if ("true".equals(map.get("honorDeletes")))
                result.honorDeletes = true;
            else if ("false".equals(map.get("honorDeletes")))
                result.honorDeletes = false;

            if (map.get("lastRebuilt") != null) {
                try {
                    result.lastRebuilt = Long.parseLong(map.get("lastRebuilt"), 10);
                } catch (RuntimeException e) {
                    // ignore it
                }
            }

            if (map.get("lastCleared") != null) {
                try {
                    result.lastCleared = Long.parseLong(map.get("lastCleared"),10);
                } catch (RuntimeException e) {
                    // ignore it
                }
            }

            if (map.get("batchSize") != null) {
                try {
                    result.batchSize = Integer.parseInt(map.get("batchSize"), 10);
                } catch (RuntimeException e) {
                    // ignore it
                }
            }
            return result;
        }
        else return null;
    }

    public void merge(EventIndexBackendConfiguration that) {
        if (this.name == null || !this.name.equals(that.name))
            throw new IllegalStateException();
        if (that.status != null)
            this.status = that.status;
        if (that.asyncUpdates != null)
            this.asyncUpdates = that.asyncUpdates;
        if (that.honorDeletes != null)
            this.honorDeletes = that.honorDeletes;
        if (that.lastRebuilt != null) {
            if (this.lastRebuilt == null)
                this.lastRebuilt = that.lastRebuilt;
            else
                this.lastRebuilt = Math.max(this.lastRebuilt, that.lastRebuilt);
        }
        if (that.lastCleared != null) {
            if (this.lastCleared == null)
                this.lastCleared = that.lastCleared;
            else
                this.lastCleared = Math.max(this.lastCleared, that.lastCleared);
        }
        if (that.batchSize != null)
            this.batchSize = that.batchSize;
    }
}
