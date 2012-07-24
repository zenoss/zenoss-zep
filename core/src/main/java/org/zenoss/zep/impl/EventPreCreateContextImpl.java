/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.zep.impl;

import org.zenoss.protobufs.zep.Zep.ZepRawEvent;
import org.zenoss.zep.ClearFingerprintGenerator;
import org.zenoss.zep.plugins.EventPreCreateContext;

import java.util.HashSet;
import java.util.Set;

public class EventPreCreateContextImpl implements EventPreCreateContext {

    private Set<String> clearClasses = new HashSet<String>();
    private ClearFingerprintGenerator clearFingerprintGenerator = null;

    public EventPreCreateContextImpl() {
    }

    public EventPreCreateContextImpl(ZepRawEvent rawEvent) {
        if (rawEvent == null) {
            throw new NullPointerException();
        }
        this.clearClasses.addAll(rawEvent.getClearEventClassList());
    }

    @Override
    public Set<String> getClearClasses() {
        return this.clearClasses;
    }

    @Override
    public void setClearClasses(Set<String> clearClasses) {
        if (clearClasses == null) {
            throw new NullPointerException();
        }
        this.clearClasses = clearClasses;
    }

    @Override
    public ClearFingerprintGenerator getClearFingerprintGenerator() {
        return this.clearFingerprintGenerator;
    }

    @Override
    public void setClearFingerprintGenerator(ClearFingerprintGenerator clearFingerprintGenerator) {
        this.clearFingerprintGenerator = clearFingerprintGenerator;
    }

    @Override
    public String toString() {
        return String.format("EventPreCreateContextImpl [clearClasses=%s, clearFingerprintGenerator=%s]", clearClasses,
                this.clearFingerprintGenerator);
    }

}
