/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2018, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

package org.zenoss.zep.zing;

import org.junit.Test;
import static org.junit.Assert.*;

public class ZingConfigTest {

    @Test
    public void testValidateConfig() {
        String tnt = "acme";
        String src = "aus";
        String prj = "zing-zing";
        String topic = "events";
        String emulator = "1.1.1.1:8085";
        String credsPath = "";
        String minSev = "";
        boolean enabled = true;
        boolean useEmulator = true;

        ZingConfig cfg = new ZingConfig(enabled, useEmulator, tnt, src, prj, topic, emulator, credsPath, minSev);

        // config without tenant, source, project or topic is invalid
        assertTrue(cfg.validate());
        cfg.setTenant("");
        assertFalse(cfg.validate());
        cfg.setTenant(tnt);
        cfg.setSource("");
        assertFalse(cfg.validate());
        cfg.setSource(src);
        cfg.setProject("");
        assertFalse(cfg.validate());
        cfg.setProject(prj);
        cfg.setTopic("");
        assertFalse(cfg.validate());
        cfg.setTopic(topic);
        assertTrue(cfg.validate());

        // If use emulator is true, emulator url cant be empty
        cfg.setUseEmulator(true);
        cfg.setEmulatorHostAndPort("");
        assertFalse(cfg.validate());
        cfg.setEmulatorHostAndPort(emulator);
        assertTrue(cfg.validate());

        // If a creds path exists, it must point to a readable file
        cfg.setCredentialsPath("/a/b/c");
        assertFalse(cfg.validate());
    }
}
