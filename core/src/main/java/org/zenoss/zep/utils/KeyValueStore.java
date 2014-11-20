package org.zenoss.zep.utils;

import com.google.common.base.Function;

import java.io.IOException;
import java.util.Map;

public interface KeyValueStore {
    void store(byte[] key, byte[] value) throws IOException;
    byte[] load(byte[] key) throws IOException;
    void checkAndSetAll(Function<Map<byte[],byte[]>,Map<byte[],byte[]>> mapper) throws IOException;
    Map<byte[], byte[]> loadAll() throws IOException;
}
