/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.engine.IChunkAccessor;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.relation.accesspath.EmptyCloseableIterator;

import cutthecrap.utils.striterators.ICloseableIterator;


/**
 * An empty {@link IChunkMessage}. This is used to kick off the optional last
 * evaluation phase for an operator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyChunkMessage<E> implements IChunkMessage<E>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    final private IQueryClient queryController;

    final private UUID queryControllerId;
    
    final private UUID queryId;

    final private int bopId;
    
    final private int partitionId;

    final private boolean lastInvocation;
    
    @Override
    public IQueryClient getQueryController() {
        return queryController;
    }

    @Override
    public UUID getQueryControllerId() {
        return queryControllerId;
    }
    
    @Override
    public UUID getQueryId() {
        return queryId;
    }

    @Override
    public int getBOpId() {
        return bopId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }
    
    @Override
    public boolean isLastInvocation() {
        return true; // Always.
    }
    
    @Override
    public boolean isMaterialized() {
        return true;
    }

    @Override
    public int getSolutionCount() {
        return 0;
    }
    
    public int getBytesAvailable() {
        return 0;
    }

    @Override
    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + ",controller="
                + queryController + ",lastInvocation=" + lastInvocation + "}";

    }

    /**
     * 
     * @param queryController
     * @param queryId
     * @param bopId
     * @param partitionId
     */
    public EmptyChunkMessage(final IQueryClient queryController,
            final UUID queryId, final int bopId, final int partitionId,
            final boolean lastInvocation) {

        if (queryController == null)
            throw new IllegalArgumentException();

        if (queryId == null)
            throw new IllegalArgumentException();

        this.queryController = queryController;
        try {
            this.queryControllerId = queryController.getServiceUUID();
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
        
        this.queryId = queryId;

        this.bopId = bopId;

        this.partitionId = partitionId;
        
        this.lastInvocation = lastInvocation;

    }

    @Override
    public void materialize(FederatedRunningQuery runningQuery) {
        // NOP
    }

    @Override
    public void release() {
        // NOP
    }

    @Override
    public IChunkAccessor<E> getChunkAccessor() {

        return new IChunkAccessor<E>() {

            public ICloseableIterator<E[]> iterator() {

                return new EmptyCloseableIterator<E[]>();
                
            }
            
        };
        
    }
    
}
