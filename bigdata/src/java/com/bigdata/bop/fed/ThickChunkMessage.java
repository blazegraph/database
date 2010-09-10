/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 9, 2010
 */

package com.bigdata.bop.fed;

import java.io.Serializable;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.btree.data.DefaultLeafCoder;
import com.bigdata.relation.accesspath.IAsynchronousIterator;


/**
 * A thick version of this interface in which the chunk is sent inline with the
 * RMI message.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThickChunkMessage implements IChunkMessage, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    final private IQueryClient queryController;

    final private long queryId;

    final private int bopId;
    
    final private int partitionId;

    final private byte[] data;

    public IQueryClient getQueryController() {
        return queryController;
    }

    public long getQueryId() {
        return queryId;
    }

    public int getBOpId() {
        return bopId;
    }

    public int getPartitionId() {
        return partitionId;
    }
    
    public boolean isMaterialized() {
        return true;
    }

    public int getBytesAvailable() {
        return data.length;
    }

    /**
     * 
     * @param queryController
     * @param queryId
     * @param bopId
     * @param partitionId
     * @param data
     */
    public ThickChunkMessage(final IQueryClient queryController,
            final long queryId, final int bopId, final int partitionId,
            final byte[] data) {

        if (queryController == null)
            throw new IllegalArgumentException();

        if (data == null)
            throw new IllegalArgumentException();

        // do not send empty chunks.
        if (data.length == 0)
            throw new IllegalArgumentException();

        this.queryController = queryController;
        
        this.queryId = queryId;

        this.bopId = bopId;

        this.partitionId = partitionId;
        
        this.data = data;

    }

    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + "}";

    }

    public void materialize(FederatedRunningQuery runningQuery) {
        // NOP
    }

    /**
     * FIXME Provide in place decompression and read out of the binding sets.
     * Look at the {@link DefaultLeafCoder} pattern.
     */
    public IAsynchronousIterator<IBindingSet[]> iterator() {

        throw new UnsupportedOperationException();

    }

}
