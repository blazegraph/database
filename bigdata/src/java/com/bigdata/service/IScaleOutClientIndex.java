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
 * Created on Mar 31, 2009
 */

package com.bigdata.service;

import java.util.Iterator;

import com.bigdata.btree.ITuple;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.StaleLocatorException;

/**
 * A client-side view of a scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IScaleOutClientIndex extends IClientIndex {

    /**
     * Resolve the data service to which the index partition is mapped.
     * 
     * @param pmd
     *            The index partition locator.
     * 
     * @return The data service and never <code>null</code>.
     * 
     * @throws RuntimeException
     *             if none of the data services identified in the index
     *             partition locator record could be discovered.
     */
    IDataService getDataService(final PartitionLocator pmd);

    /**
     * Returns an iterator that will visit the {@link PartitionLocator}s for
     * the specified scale-out index key range.
     * 
     * @see AbstractScaleOutFederation#locatorScan(String, long, byte[], byte[],
     *      boolean)
     * 
     * @param ts
     *            The timestamp that will be used to visit the locators.
     * @param fromKey
     *            The scale-out index first key that will be visited
     *            (inclusive). When <code>null</code> there is no lower bound.
     * @param toKey
     *            The first scale-out index key that will NOT be visited
     *            (exclusive). When <code>null</code> there is no upper bound.
     * @param reverseScan
     *            <code>true</code> if you need to visit the index partitions
     *            in reverse key order (this is done when the partitioned
     *            iterator is scanning backwards).
     * 
     * @return The iterator. The value returned by {@link ITuple#getValue()}
     *         will be a serialized {@link PartitionLocator} object.
     */
    Iterator<PartitionLocator> locatorScan(final long ts, final byte[] fromKey,
            final byte[] toKey, final boolean reverseScan);

    /**
     * Notifies the client that a {@link StaleLocatorException} was received.
     * The client will use this information to refresh the
     * {@link IMetadataIndex}.
     * 
     * @param ts
     *            The timestamp of the metadata index view from which the
     *            locator was obtained.
     * @param locator
     *            The locator that was stale.
     * @param cause
     *            The reason why the locator became stale (split, join, or
     *            move).
     * 
     * @throws RuntimeException
     *             unless the timestamp given is {@link ITx#UNISOLATED} or
     *             {@link ITx#READ_COMMITTED} since stale locators do not occur
     *             for other views.
     */
    void staleLocator(final long ts, final PartitionLocator locator,
            final StaleLocatorException cause);
    
    /**
     * Return the object used to access the services in the connected
     * federation.
     */
    public AbstractScaleOutFederation getFederation();

}
