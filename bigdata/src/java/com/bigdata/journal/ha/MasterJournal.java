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

package com.bigdata.journal.ha;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAMessage;
import com.bigdata.ha.SlaveJournal;
import com.bigdata.ha.HAMessage.HAAddWrite;
import com.bigdata.ha.HAMessage.HACommitRootBlock;
import com.bigdata.ha.HAMessage.HAExtendFile;
import com.bigdata.ha.HAMessage.HASlaveMessage;
import com.bigdata.ha.HAMessage.HAUpdateAllocationBlock;
import com.bigdata.ha.HAMessage.HAUpdateMetaAllocation;
import com.bigdata.io.WriteCache;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.rwstore.RWStore;

/**
 * The MasterJournal knows who its linked IBufferStrategy is and it talks to
 * SlaveJournals keeping them in sync.
 * 
 * It may talk either directly (for testing and local replication) or indirectly
 * using Socket or RMI-based protocols.
 * 
 * The master talks directly, but each request is converted to a serializable message
 * to run on an Executor service
 * 
 * TODO: The MasterJournal will provide a protocol to interact with a JournalFederation
 * and thus may control the master.
 * 
 * @author Martyn Cutcher
 *
 */
public class MasterJournal {
	protected static final Logger log = Logger.getLogger(MasterJournal.class);

	private ArrayList<SlaveJournal> m_directSlaves = new ArrayList<SlaveJournal>();
	private ArrayList<ObjectOutputStream> m_remoteSlaves = new ArrayList<ObjectOutputStream>();
	private IBufferStrategy m_master;
	
	public MasterJournal(IBufferStrategy master) {
		m_master = master;
	}
	
	public void addSlave(SlaveJournal slave) {
		m_directSlaves.add(slave);
	}
	public void addSlave(Socket slave) {
		try {
			m_remoteSlaves.add(new ObjectOutputStream(slave.getOutputStream()));
		} catch (IOException e) {
			// remote slave not added if socket exception
			log.warn("Unable to open stream to slave", e);
		}
	}
	
	protected void processMessage(HASlaveMessage msg) {		
		Iterator<SlaveJournal> direct = m_directSlaves.iterator();
		while (direct.hasNext()) {
			msg.apply(direct.next());
		}
		
		Iterator<ObjectOutputStream> remote = m_remoteSlaves.iterator();
		while (remote.hasNext()) {
			try {
				remote.next().writeObject(msg);
			} catch (IOException e) {
				/*
				 * TODO: this must propagate error to master/zookeeper
				 */
				log.warn("Unable to send message to slave", e);
			}
		}
	}

	/**
	 * It is assumed that the data has already been validated
	 * 
	 * @param address where data should be added
	 * @param data to be written
	 * @param chk the checksum to be appended
	 */
	public void addWrite(WriteCache wc) {
		// processMessage(new SocketMessage.HAWriteMessage(wc));
	}

	/**
	 * It is assumed that when a rootblock is sent, this implies a commit, so the buffer strategy
	 * must flush any cached writes before updating the rootblock.
	 * 
	 * For the DiskWORM, a valid rootblock is all that is required for the Slave to be able to
	 * become a read/write Master.  See updateMetaAllocation for issues with DiskRW
	 */
	public void commitRootBlock(IRootBlockView rootBlock) {
		processMessage(new HAMessage.HACommitRootBlock(rootBlock));
	}

	/**
	 * For a RWStrategy the allocation block cannot be updated using addWrite, since the in-memory
	 * blocks must be updated to allow address decoding to support read-access.  For non RWStrategies
	 * this should never be requested.  The request to the RWStrategy would be generated as part of
	 * the master commit.
	 */
	public void updateAllocationBlock(int index, long addr, byte[] data) {
		processMessage(new HAMessage.HAUpdateAllocationBlock(index, addr, data));
	}

	/**
	 * The meta allocation must be updated if the DiskRW journal is to be able to make writes.  If the Slave
	 * was to become the master it should probably re-open to the current commit, thus ensuring correct
	 * meta-allocation.  In thich case this interface would not be required.  However, it seems a little
	 * odd that in all other respects other than meta-allocation that the slave would be in-sync with the
	 * master. 
	 */
	public void updateMetaAllocation(long addr, byte[] data) {
		processMessage(new HAMessage.HAUpdateMetaAllocation(addr, data));
	}

	/**
	 * The journal file must be extended.  For the RWStrategy this will also shift the current allocation blocks
	 * and update the root block.
	 */
	public void extendFile(long fsize) {
		processMessage(new HAMessage.HAExtendFile(fsize));
	}
}
