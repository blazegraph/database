/**

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

 Contact:
 SYSTAP, LLC
 4501 Tower Road
 Greensboro, NC 27410
 licenses@bigdata.com

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; version 2 of the License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Jun 18, 2006
 */
package com.bigdata.service;

import java.io.File;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase2;

import com.bigdata.service.ResourceService.ReadResourceTask;

/**
 * Test verifies the ability to transmit a file using the
 * {@link ResourceService}.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */
public class TestResourceService extends TestCase2 {

    /**
     * 
     */
    public TestResourceService() {
        super();
    }

    public TestResourceService(String name) {
        super(name);
    }

    public void test_sendFile() throws Exception {

        final UUID allowedUUID = UUID.randomUUID();

        final File allowedFile = new File(
                "src/test/com/bigdata/service/testSendFile.seg");

        assertTrue("Could not locate file: " + allowedFile, allowedFile
                .exists());

        final File tmpFile = File.createTempFile(allowedFile.getName(), ".tmp");

        final ResourceService service = new ResourceService() {

            @Override
            protected File getResource(UUID uuid) {

                if (allowedUUID.equals(uuid)) {

                    // allowed.
                    return allowedFile;

                }

                log.warn("Not allowed: " + uuid);

                // Not allowed.
                return null;

            }

        };

        try {

            service.awaitRunning(100, TimeUnit.MILLISECONDS);

            assertTrue(service.isOpen());

            assertEquals(tmpFile, new ReadResourceTask(InetAddress
                    .getLocalHost(), service.port, allowedUUID, tmpFile).call());

            if (log.isInfoEnabled())
                log.info(service.counters.getCounters());
            
        } finally {

            if (tmpFile.exists()) {

                // delete tmp file.
                tmpFile.delete();

            }

            // shutdown the service.
            service.shutdownNow();

            // verify service is down.
            assertFalse(service.isOpen());

        }

    }

}
