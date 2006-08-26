/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jun 18, 2006
 */
package org.CognitiveWeb.bigdata;

import net.jini.discovery.DiscoveryEvent;

import org.CognitiveWeb.bigdata.jini.TestServiceDiscovery.TestServerImpl;
import org.CognitiveWeb.concurrent.locking.TxDag;
import org.apache.log4j.Logger;

/**
 * Lock server.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 * 
 * @todo Register as jini service.
 * @todo Use custom JERI RMI protocol.
 * @todo Maintain local copy of the catalog segment.
 * @todo Update the catalog as segments come online or die out.
 * @todo Consider the separation between the LockServer and the
 *       SegmentManager.
 * @todo Use S and X modes with Queue per Segment to handle locking model.
 *       This will transparently use the {@link TxDag} behind the scenes.
 */

public class LockServer implements ILockManager //, DiscoveryListener
{

    Logger log = Logger.getLogger(LockServer.class);
    
    //      protected LeaseRenewalManager leaseManager = new LeaseRenewalManager();

    /**
     * Server startup performs asynchronous multicast lookup discovery. The
     * {@link #discovered(DiscoveryEvent)}method is invoked asynchronously
     * to register {@link TestServerImpl}instances.
     */
//    public LockServer() {
//
//        try {
//
//            LookupDiscovery discover = new LookupDiscovery(
//                    LookupDiscovery.ALL_GROUPS);
//            
//            discover.addDiscoveryListener(this);
//            
//        } catch (IOException ex) {
//            
//            throw new RuntimeException(ex);
//            
//        }
//
//    }
//
//    /**
//     * Register the {@link LockServerProxy}.
//     */
//    public void discovered(DiscoveryEvent evt) {
//    
//        TestPageServer.log.info("");
//
//        ServiceRegistrar registrar = evt.getRegistrars()[0];
//        // At this point we have discovered a lookup service
//
//        // Create information about a service
//        ServiceItem item = new ServiceItem(null, new LockServerProxy(),
//                null);
//
//        // Export a service
//        try {
//            ServiceRegistration reg = registrar.register(item, Lease.FOREVER);
//        }
//        catch( RemoteException ex ) {
//            throw new RuntimeException( ex );
//        }
//
//    }
//
//    /**
//     * Log a message.
//     */
//    public void discarded(DiscoveryEvent arg0) {
//        TestPageServer.log.info("");
//    }

    public void lock( long txId, int segmentId, boolean readOnly, long timeout ) {
        log.info("Lock requested: tx="+txId+", segment="+segmentId+", readOnly="+readOnly+", timeout="+timeout);
    }
    
    public void release( long txId ) {
        log.info("Release locks: tx="+txId);
    }

//    /**
//     * A lightweight proxy for a {@link LockServer}.
//     * 
//     * @version $Id$
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson </a>
//     * 
//     * @todo Direct operations over RMI to the {@link LockServer}.
//     */
//
//    public static class LockServerProxy implements ILockManager {
//        
//        public void lock( long txId, int segmentId, boolean readOnly, long timeout ) {
//            TestPageServer.log.info("Lock requested: tx="+txId+", segment="+segmentId+", readOnly="+readOnly+", timeout="+timeout);
//        }
//        
//        public void release( long txId ) {
//            TestPageServer.log.info("Release locks: tx="+txId);
//        }
//        
//    }
    
}