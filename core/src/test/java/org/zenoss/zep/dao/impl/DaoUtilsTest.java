/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2012, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.dao.impl;

import org.junit.Test;
import org.springframework.dao.DeadlockLoserDataAccessException;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DaoUtilsTest {
    @Test
    public void testDeadlockRetry() throws Exception {
        final AtomicInteger i = new AtomicInteger();
        final int returnVal = new Random().nextInt();
        int result = DaoUtils.deadlockRetry(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                if (i.incrementAndGet() < 5) {
                    throw new DeadlockLoserDataAccessException("My fake exception", null);
                }
                return returnVal;
            }
        });
        assertEquals(i.get(), 5);
        assertEquals(result, returnVal);
    }

    @Test
    public void testDeadlockRetryAllFailed() throws Exception {
        final AtomicInteger i = new AtomicInteger();
        try {
            DaoUtils.deadlockRetry(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    throw new DeadlockLoserDataAccessException(String.valueOf(i.incrementAndGet()), null);
                }
            });
            fail("Should have thrown an exception after 5 retries");
        } catch (DeadlockLoserDataAccessException e) {
            assertEquals("5", e.getMessage());
        }
    }

    @Test
    public void testDeadlockRetryNestedException() throws Exception {
        final AtomicInteger i = new AtomicInteger();
        final int returnVal = new Random().nextInt();
        int result = DaoUtils.deadlockRetry(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                if (i.incrementAndGet() < 5) {
                    throw new RuntimeException(new DeadlockLoserDataAccessException("My fake exception", null));
                }
                return returnVal;
            }
        });
        assertEquals(i.get(), 5);
        assertEquals(result, returnVal);
    }

    @Test
    public void testDeadlockRetryOtherException() throws Exception {
        final AtomicInteger i = new AtomicInteger();
        try {
            DaoUtils.deadlockRetry(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    i.incrementAndGet();
                    throw new RuntimeException("Bad exception - no retry");
                }
            });
            fail("Should have thrown an exception after first retry");
        } catch (RuntimeException e) {
            assertEquals(1, i.get());
            assertEquals("Bad exception - no retry", e.getMessage());
        }
    }
}
