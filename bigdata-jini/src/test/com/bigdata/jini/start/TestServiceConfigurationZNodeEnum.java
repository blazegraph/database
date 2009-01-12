/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 12, 2009
 */

package com.bigdata.jini.start;

import com.bigdata.service.jini.TransactionServer;

import junit.framework.TestCase2;

/**
 * Test suite for {@link ServiceConfigurationZNodeEnum}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestServiceConfigurationZNodeEnum extends TestCase2 {

    /**
     * 
     */
    public TestServiceConfigurationZNodeEnum() {
       
    }

    /**
     * @param arg0
     */
    public TestServiceConfigurationZNodeEnum(String arg0) {
       
        super(arg0);
        
    }

    final String serviceConfigZPath = "test-fed/" + BigdataZooDefs.CONFIG + "/"
            + TransactionServer.class.getName();

    /**
     * <p>
     * The behavior can be best interpreted with reference to an example:
     * 
     * <pre>
     * test-fed 
     *   locks 
     *     serviceConfigMonitor 
     *       com.bigdata.service.jini.TransactionServer 
     *         lock0000000000 (Ephemeral) 
     *     createPhysicalService 
     *   config 
     * W   com.bigdata.service.jini.TransactionServer {TransactionServiceConfiguration}
     * W     logicalService0000000000 
     * W       physicalServices 
     * W         abde9b91-24d5-4dc5-9bbf-41d7e7cac272 (Ephemeral) 
     *         masterElection 
     *           lock0000000000 (Ephemeral) 
     * </pre>
     * 
     * In this example, the <code>W</code> appears at the start of each
     * watched znode for the {@link TransactionServiceConfiguration} znode. The
     * master election znode can be seen directly below that.
     */
    public void test01() {
        
        assertEquals(ServiceConfigurationZNodeEnum.ServiceConfiguration,
                ServiceConfigurationZNodeEnum
                        .getType(serviceConfigZPath, serviceConfigZPath));

        assertEquals(ServiceConfigurationZNodeEnum.LogicalService,
                ServiceConfigurationZNodeEnum.getType(serviceConfigZPath,
                        serviceConfigZPath + "/"
                                + BigdataZooDefs.LOGICAL_SERVICE_PREFIX
                                + "0000000000"));

        assertEquals(ServiceConfigurationZNodeEnum.PhysicalServicesContainer,
                ServiceConfigurationZNodeEnum.getType(serviceConfigZPath,
                        serviceConfigZPath + "/"
                                + BigdataZooDefs.LOGICAL_SERVICE_PREFIX
                                + "0000000000"+"/"
                                + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER));


        assertEquals(ServiceConfigurationZNodeEnum.PhysicalService,
                ServiceConfigurationZNodeEnum.getType(serviceConfigZPath,
                        serviceConfigZPath + "/"
                                + BigdataZooDefs.LOGICAL_SERVICE_PREFIX
                                + "0000000000" + "/"
                                + BigdataZooDefs.PHYSICAL_SERVICES_CONTAINER
                                + "/" + "abde9b91-24d5-4dc5-9bbf-41d7e7cac272"));

        assertEquals(ServiceConfigurationZNodeEnum.MasterElection,
                ServiceConfigurationZNodeEnum.getType(serviceConfigZPath,
                        serviceConfigZPath + "/"
                                + BigdataZooDefs.LOGICAL_SERVICE_PREFIX
                                + "0000000000" + "/"
                                + BigdataZooDefs.MASTER_ELECTION
                                ));

        assertEquals(ServiceConfigurationZNodeEnum.MasterElectionLock,
                ServiceConfigurationZNodeEnum.getType(serviceConfigZPath,
                        serviceConfigZPath + "/"
                                + BigdataZooDefs.LOGICAL_SERVICE_PREFIX
                                + "0000000000" + "/"
                                + BigdataZooDefs.MASTER_ELECTION
                                + "/" + "lock0000000000"));

    }

}
