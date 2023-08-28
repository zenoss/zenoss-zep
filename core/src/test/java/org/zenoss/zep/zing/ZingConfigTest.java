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
        Integer maxPubsubMessSize = 0;
        Integer maxEventFieldLength = 0;
        boolean enabled = true;
        boolean useEmulator = true;
        boolean usePubsubLite = true;
        long pubsubLiteProjectNumber = 123456;
        String pubsubLiteLocation = "australia-southeast1-a";


        ZingConfig cfg = new ZingConfig(enabled, useEmulator, usePubsubLite, tnt, src, prj, topic, pubsubLiteProjectNumber, pubsubLiteLocation, emulator, credsPath, minSev, maxPubsubMessSize, maxEventFieldLength);

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

        // Test pubsub lite options
        cfg.setUsePubsubLite(true);
        assertTrue(cfg.validate());
        cfg.setPubsubLiteProjectNumber(0);
        assertFalse(cfg.validate());
        cfg.setPubsubLiteProjectNumber(12345);
        assertTrue(cfg.validate());
        cfg.setPubsubLiteLocation("");
        assertFalse(cfg.validate());
        cfg.setPubsubLiteLocation("australia-southweat1-a");

        // If a creds path exists, it must point to a readable file
        cfg.setCredentialsPath("/a/b/c");
        assertFalse(cfg.validate());
    }

    @Test
    public void testPubsubLiteDefaultConfig() {
        String tnt = "acme";
        String src = "aus";
        String prj = "zing-zing";
        String topic = "events";
        String emulator = "1.1.1.1:8085";
        String credsPath = "";
        String minSev = "";
        Integer maxPubsubMessSize = 0;
        Integer maxEventFieldLength = 0;
        boolean enabled = true;
        boolean useEmulator = false;
        boolean usePubsubLite = false;
        long pubsubLiteProjectNumber = 0;
        String pubsubLiteLocation = "";

        ZingConfig cfg = new ZingConfig(enabled, useEmulator, usePubsubLite, tnt, src, prj, topic, pubsubLiteProjectNumber, pubsubLiteLocation, emulator, credsPath, minSev, maxPubsubMessSize, maxEventFieldLength);

        assertTrue(cfg.validate());

        // will fail, because we don't recognize this project and can't provide proper defaults
        cfg.setUsePubsubLite(true);
        cfg.setProject("zing-zing");
        assertFalse(cfg.validate());

        // changing it to a proper project should make it succeed.
        cfg.setProject("zing-dev-197522");
        cfg.setDefaults();
        assertTrue(cfg.validate());
    }


}
