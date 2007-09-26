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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.util.UUID;

import net.jini.core.lookup.ServiceID;

import org.apache.log4j.Logger;

import com.bigdata.journal.CommitRecordIndex.Entry;

/**
 * Interface allowing clients to connect to {@link IBigdataFederation}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBigdataClient {

    public static final Logger log = Logger.getLogger(IBigdataClient.class);

    /**
     * Connect to a bigdata federation. If the client is already connected, then
     * the existing connection is returned.
     * 
     * @return The federation.
     * 
     * @todo determine how a federation will be identified, e.g., by a name that
     *       is an {@link Entry} on the {@link MetadataServer} and
     *       {@link DataServer} service descriptions and provide that name
     *       attribute here. Note that a {@link MetadataService} can failover,
     *       so the {@link ServiceID} for the {@link MetadataService} is not the
     *       invariant, but rather the name attribute for the federation.
     */
    public IBigdataFederation connect();
    
    /**
     * Disconnects from any connected federation(s) and then terminate any
     * background processing that is being performed on the behalf of the
     * client.
     */
    public void terminate();

    /**
     * Resolve the service identifier to an {@link IDataService}.
     * <p>
     * Note: Whether the returned object is a proxy or the service
     * implementation depends on whether the federation is embedded (in process)
     * or distributed (networked).
     * 
     * @param serviceUUID
     *            The identifier for a {@link IDataService}.
     * 
     * @return The {@link IDataService} or <code>null</code> iff the
     *         {@link IDataService} could not be discovered from its identifier.
     */
    public IDataService getDataService(UUID serviceUUID);

    /**
     * Return the metadata service.
     * <p>
     * Note: Whether the returned object is a proxy or the service
     * implementation depends on whether the federation is embedded (in process)
     * or distributed (networked).
     * 
     * @return The metadata service.
     */
    public IMetadataService getMetadataService();
        
}
