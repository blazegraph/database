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
 * Created on Apr 23, 2007
 */

package com.bigdata.scaleup;

import java.util.UUID;

import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.Journal;

/**
 * A description of the metadata state for a partition of a scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPartitionMetadata {

    /**
     * The unique partition identifier.
     */
    public int getPartitionId();

    /**
     * The ordered list of data services on which data for this partition will
     * be written and from which data for this partition may be read.
     * 
     * @todo refactor into a dataService UUID (required) and an array of zero or
     *       more media replication services for failover.
     */
    public UUID[] getDataServices();
    
    /**
     * Zero or more files containing {@link Journal}s or {@link IndexSegment}s
     * holding live data for this partition. The entries in the array reflect
     * the creation time of the index segments. The earliest resource is listed
     * first. The most recently created resource is listed last. Only the
     * {@link ResourceState#Live} resources must be read in order to provide a
     * consistent view of the data for the index partition.
     * {@link ResourceState#Dead} resources will eventually be scheduled for
     * restart-safe deletion.
     * 
     * @see ResourceState
     */
    public IResourceMetadata[] getResources();

}
