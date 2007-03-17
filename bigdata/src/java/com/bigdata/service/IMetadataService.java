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
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

import java.net.InetSocketAddress;

/**
 * A metadata service for a named index.
 * <p>
 * The metadata service maintains locator information for the data service
 * instances responsible for each partition in the named index.  Partitions
 * are automatically split when they overflow (~200M) and joined when they
 * underflow (~50M).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IMetadataService {

    /**
     * The approximate number of entries in the index (non-transactional).
     */
    public int getEntryCount();
    
    /**
     * The approximate number of entries in the index for the specified key
     * range (non-transactional).
     * 
     * @param fromKey
     * @param toKey
     * @return
     */
    public int rangeCount(byte[] fromKey,byte[] toKey);
    
    /**
     * Return the address of the {@link IDataService} that has current primary
     * responsibility for the index partition that includes the specified key.
     * 
     * @param key
     *            The key.
     * 
     * @return The locator for the {@link IDataService} with primary
     *         responsibility for the index partition in which that key would be
     *         located.
     * 
     * @todo return primary and secondary data service locators with lease.
     * 
     * @todo return primary and secondary data service locators with lease for
     *       the index partition that would contain the key plus some number of
     *       index partitions surrounding that partition.
     */
    public InetSocketAddress getDataService(byte[] key);
    
}
