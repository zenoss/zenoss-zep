package org.zenoss.zep.utils;

public class RedisTransactionCollision extends RuntimeException {
    public RedisTransactionCollision(String msg) {
        super(msg);
    }
}
