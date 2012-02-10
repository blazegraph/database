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
import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.engine.IChunkAccessor;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.relation.accesspath.EmptyAsynchronousIterator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;


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
    
    public IQueryClient getQueryController() {
        return queryController;
    }

    public UUID getQueryControllerId() {
        return queryControllerId;
    }
    
    public UUID getQueryId() {
        return queryId;
    }

    public int getBOpId() {
        return bopId;
    }

    public int getPartitionId() {
        return partitionId;
    }
    
    public boolean isLastInvocation() {
        return true; // Always.
    }
    
    public boolean isMaterialized() {
        return true;
    }

    public int getSolutionCount() {
        return 0;
    }
    
    public int getBytesAvailable() {
        return 0;
    }

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

    public void materialize(FederatedRunningQuery runningQuery) {
        // NOP
    }

    public void release() {
        // NOP
    }

    public IChunkAccessor<E> getChunkAccessor() {

        return new IChunkAccessor<E>() {

            public IAsynchronousIterator<E[]> iterator() {

                return new EmptyAsynchronousIterator<E[]>();
                
            }
            
        };
        
    }
    
}
