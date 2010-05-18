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
 * Created on Apr 23, 2010
 */

package com.bigdata.journal.ha;

import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.Journal;

public class MockQuorumManager implements QuorumManager {

    private final int k;

    private final int index;
    
    private long token;
    
    final Journal[] stores;

    final Quorum quorum;

	private IBufferStrategy bufferStrategy;
	private HAServer haServer;
    
    public int replicationFactor() {

        return k;
        
    }

    public boolean isHighlyAvailable() {
        
        return k > 1;
        
    }
    
    public MockQuorumManager(final int index, final Journal[] stores) {

        this.k = stores.length;
        this.index = index;
        this.stores = stores;
        this.quorum = newQuorum(index, stores);

        // Start the quorum as met on token ZERO (0).
        token = 0L;

    }

    /**
     * Factory for the {@link Quorum} implementations.
     */
    protected Quorum newQuorum(final int index, final Journal[] stores) {
        
        return new MockQuorumImpl(index, stores);
        
    }
    
    public void assertQuorum(long token) {

        if (token != this.token)
            throw new IllegalStateException();
        
    }

    /** Assumes that the quorum is met. */
    public Quorum awaitQuorum() throws InterruptedException {
     
        return getQuorum();
        
    }

    public Quorum getQuorum() {
        
        return quorum;
        
    }

    public void terminate() {
        
        // NOP.
        
    }

	public IBufferStrategy getLocalBufferStrategy() {
		return bufferStrategy;
	}

//	@Override
	public void setLocalBufferStrategy(IBufferStrategy strategy) {
		bufferStrategy = strategy;
	}

	public HAConnect getHAConnect() {
		// TODO Auto-generated method stub
		return null;
	}

	public HAServer getHAServer() {
		return haServer;
	}

	/**
	 * Ensure HAServer is created and started, ready to process upstream messages.
	 */
	public HAServer establishHAServer(IHAClient haClient) {
		if (haServer == null) {
	        final int index = quorum.getIndex();
	
	        // if (index + 1 < k) { // this would rule out the final node!
	
	        final HAGlue haGlueService = quorum.getHAGlue(index);
	
	        // Our local service listening for upstream messages - needed for all non-master nodes
	        haServer = new HAServer(//
	        		haGlueService.getWritePipelineAddr(),//
	        		// haGlueService.getWritePipelinePort(),//
	                haClient,//
	                false // No message drive
	                );
	        
	        haServer.start(); // must process accept messages to establish socket connection
		}
		
		return haServer;
	}
    
}
