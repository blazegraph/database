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

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;

/**
 * A description of the metadata state for a partition of a scale-out index.
 * <p>
 * Each index partition has a distinct partitionId. This partitionId is assigned
 * by a centralized service - the {@link IMetadataService} for the scale-out
 * index for that index partition. A centralized service is required in order to
 * obtain distinct int32 partition identifiers because those partition
 * identifiers are used in turn to support scale-out partition local
 * {@link ICounter}s - the partitionId forms the upper word of the int64
 * counter value.
 * <p>
 * The scale-out index name and the left and right separator keys are a complete
 * identifier for an index partition - any two index partitions which share
 * those three properties MUST be the same index partition. However, access to
 * index partitions is generally in terms of the name of the scale-out index and
 * the partitionId.  See {@link DataService#getIndexPartitionName(String, int)}
 * which returns the name under which an index partition will be registered and
 * the name that must be used when requesting operations on that index partition
 * using an {@link IDataService}.
 * <p>
 * An index partition has additional state, including:
 * <ul>
 * <li>the {@link IDataService} on which it resides.</li>
 * <li>the {@link IResourceMetadata}[] describing the resources required to
 * materialize a view of the index partition.</li>
 * <li>those resources themselves, which contain the index partition data.</li>
 * </ul>
 * <p>
 * The left and right separator keys define the half-open key range of the index
 * partition. The separator keys are available directly as the <i>keys</i> of
 * the {@link MetadataIndex}, therefore they are not stored in the index
 * partition records within the metadata index. However, the separator keys are
 * stored in the index partition description within the {@link IndexMetadata}
 * records so that they are available locally with the index partition data.
 * <p>
 * If the client knows a key or key range of interest for a scale-out index then
 * they can obtain the relevant index partition descriptions and a data service
 * locator either either by flooding the query to the {@link IDataService}s or
 * from the {@link MetadataIndex}.
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
     * Return {@link #getPartitionId()}
     */
    public int hashCode();
    
}
