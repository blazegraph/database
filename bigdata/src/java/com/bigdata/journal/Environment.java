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
package com.bigdata.journal;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import com.bigdata.journal.ha.MockSingletonQuorumManager;
import com.bigdata.journal.ha.QuorumManager;

/**
 * The Environment object is a general hook accessible to all layers
 * (Journal,Strategy,WriteCacheService etc..) within a service.
 * 
 * The incentive for this has been driven by the HA design and the need to
 * co-ordinate actions across layers that had not necessarily been developed
 * with this in mind.
 * 
 * Specifically, the Environment provides will provide access to the Quorum
 * as the first link into the HA functionality.
 * 
 * @author Martyn Cutcher
 *
 */
public class Environment {

	private AbstractJournal journal;
	private IBufferStrategy strategy;
	private QuorumManager quorumManager;
    private InetAddress writePipelineAddr;
    private int writePipelinePort;

	public Environment(AbstractJournal journal) {
		this.journal = journal;
		this.quorumManager = journal.getQuorumManager();
	}
	
	public void setWritePipeline(InetAddress addr, int port) {
		writePipelineAddr = addr;
		writePipelinePort = port;
	}
	
	public InetAddress getWritePipelineAddr() {
		return writePipelineAddr;
	}

	public int getWritePipelinePort() {
		return writePipelinePort;
	}

	public AbstractJournal getJournal() {
		return journal;
	}

	public void setQuorumManager(QuorumManager quorumManager) {
		this.quorumManager = quorumManager;
	}

	public QuorumManager getQuorumManager() {
		return quorumManager;
	}

	public void setLocalBufferStrategy(IBufferStrategy strategy) {
		this.strategy = strategy;
	}

	public boolean isHighlyAvailable() {
		return quorumManager != null && quorumManager.isHighlyAvailable();
	}

	public long getActiveFileExtent() {
		if (strategy == null) {
			throw new IllegalStateException("BufferStarteg not set in Environment");
		}
		
		return strategy.getExtent();
	}

	public IBufferStrategy getStrategy() {
		return strategy;
	}

	public void checkWritePipeline() {
        if (writePipelineAddr == null) {
            try {
                writePipelineAddr = InetAddress
                        .getByAddress(new byte[] { 127, 0, 0, 1 });
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        if (writePipelinePort == 0) {
            try {
                writePipelinePort = getPort(0/* suggestedPort */);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
       }
	}

    /**
     * Return an open port on current machine. Try the suggested port first. If
     * suggestedPort is zero, just select a random port
     */
    static protected int getPort(int suggestedPort) throws IOException {
        ServerSocket openSocket;
        try {
            openSocket = new ServerSocket(suggestedPort);
        } catch (BindException ex) {
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        }
        final int port = openSocket.getLocalPort();
        openSocket.close();
        return port;
    }

}
