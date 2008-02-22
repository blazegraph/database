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
 * Created on Apr 23, 2007
 */

package com.bigdata.mdi;

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
     * @todo data services should be placed into zones that handle replication.
     *       There should be a distinct zone for the metadata services since
     *       their data should not be co-mingled with the regular data services.
     *       The clients should get failover information from the zone not this
     *       method. Either replace this with the zone identifier and do lookup
     *       of the data service on the zone or keep this as the primary in the
     *       zone but have the zone know about failover services for the
     *       primary. This will make it much easier to move an index partition
     *       and handle failover since we will not be duplicating the data
     *       throughout the metadata index. perhaps assign a partition UUID and
     *       do lookup of the data service within the zone using that so that we
     *       do not have to update the partition description at all when we move
     *       an index partition. Alternative, retire the old partition
     *       identifier and issue a new one each time the data service chain is
     *       modified or the index partition is moved.
     */
    public UUID[] getDataServices();
    
    /**
     * Zero or more files containing {@link Journal}s or {@link IndexSegment}s
     * holding data for this index partition. The order of the resources
     * corresponds to the order in which a fused view of the index partition
     * will be read. Reads begin with the most "recent" data for the index
     * partition and stop as soon as there is a "hit" on one of the resources
     * (including a hit on a deleted index entry).
     * <p>
     * Note: Only the {@link ResourceState#Live} resources should be read in
     * order to provide a consistent view of the data for the index partition.
     * {@link ResourceState#Dead} resources will eventually be scheduled for
     * restart-safe deletion.
     * 
     * @see ResourceState
     * 
     * @todo Perhaps this information should only be kept locally in the index
     *       partitions themselves on the data service. The client certainly
     *       does not need this information in order to direct its request to
     *       the appropriate data service. This also relieves us of the chore of
     *       updating the {@link MetadataIndex} each time we overflow the
     *       journal.
     */
    public IResourceMetadata[] getResources();

}
