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

package com.bigdata.ha;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IRootBlockView;

/**
 * SlaveJournal is the object that talks between a remote MasterJournal and
 * a local Journal.
 * 
 * Whether the communication uses SmartProxy RMI, raw sockets or some other method, this will
 * pass on the data to the local Journal using direct method calls on the IBufferStrategy methods
 * 
 * The core methods provide the ability to syncronize the slave journal with the master through the
 * transference of update data.
 * 
 * Updating the rootblock will commit the slave journal.
 * 
 * For the RWStrategy, the rootblock update should be preceded with an allocation block update which will
 * include new and modified allocation blocks.
 * 
 * The use of the journal does not imply anything about cached data.  However, for the RWStrategy, the
 * allocation blocks must be synchronized with the commit, so these will be updated with a separate protocol
 * 
 * @author Martyn Cutcher
 *
 */
public class SlaveJournal implements ISlave {
	ISlave m_journal;
	
	SlaveJournal m_next;
	
	public SlaveJournal(ISlave journal) {
		m_journal = journal;
	}
	
	/**
	 * Option to chain SlaveJournals, so a message is sent on to the next Slave
	 * 
	 * @param next
	 */
	public void chain(SlaveJournal next) {
		m_next = next;
	}

	/**
	 * The master journal root block is passed for comparison to the
	 * slave journals to check their initial synchronisation state.  If the rootBlocks do
	 * not agree then either the slave or master may be at fault, as determined by the
	 * comparative commit counter.
	 * 
	 * @param rootBlock the rootBlock of the master journal
	 */
	public void checkRootBlock(IRootBlockView rootBlock) {
		m_journal.checkRootBlock(rootBlock);
	}
	
	/**
	 * It is assumed that the data has already been validated
	 * 
	 * @param address where data should be added
	 * @param data to be written
	 * @param chk the checksum to be appended
	 */
	public void addWrite(long newaddr, long oldaddr, byte[] data, long chk) {
		m_journal.addWrite(newaddr, oldaddr, data, chk);
	}

	/**
	 * It is assumed that when a rootblock is sent, this implies a commit, so the buffer strategy
	 * must flush any cached writes before updating the rootblock.
	 * 
	 * For the DiskWORM, a valid rootblock is all that is required for the Slave to be able to
	 * become a read/write Master.  See updateMetaAllocation for issues with DiskRW
	 */
	public void commitRootBlock(IRootBlockView rootBlock) {
		m_journal.commitRootBlock(rootBlock);
	}

	/**
	 * For a RWStrategy the allocation block cannot be updated using addWrite, since the in-memory
	 * blocks must be updated to allow address decoding to support read-access.  For non RWStrategies
	 * this should never be requested.  The request to the RWStrategy would be generated as part of
	 * the master commit.
	 */
	public void updateAllocationBlock(int index, long addr, byte[] data) {
		m_journal.updateAllocationBlock(index, addr, data);
	}

	/**
	 * The meta allocation must be updated if the DiskRW journal is to be able to make writes.  If the Slave
	 * was to become the master it should probably re-open to the current commit, thus ensuring correct
	 * meta-allocation.  In thich case this interface would not be required.  However, it seems a little
	 * odd that in all other respects other than meta-allocation that the slave would be in-sync with the
	 * master. 
	 */
	public void updateMetaAllocation(long addr, byte[] data) {
		m_journal.updateMetaAllocation(addr, data);
	}

	/**
	 * The journal file must be extended.  For the RWStrategy this will also shift the current allocation blocks
	 * and update the root block.
	 */
	public void extendFile(long fsize) {
		m_journal.extendFile(fsize);
	}

	/**
	 * External request to reopen the journal
	 */
	public void reopen() {
		m_journal.reopen();
	}

	/**
	 * External shutdown request
	 */
	public void shutdown() {
		m_journal.shutdown();
	}
	
	/**
	 * Connect to the master and process messages.  The message is responsible for dispatching the request
	 * to the SlaveJournal and handling the response.
	 */
	public void connectMaster(Socket server) {
		try {
			ObjectInputStream in = new ObjectInputStream(server.getInputStream());
			
			while (true) {
				HAMessage.HASlaveMessage msg = (HAMessage.HASlaveMessage) in.readObject();
				
				/**
				 * TODO: send to next in chain
				 */
				
				msg.apply(this);
			}			
		} catch (Exception e) {
			// the only exceptions should be from the stream IO, the message apply is expected to
			// handle any processing exceptions.
		}
	}
	
	/**
	 * Utility to wrap the connectMaster method within a Runnable for Threading.
	 * 
	 * @param server
	 * @return the Runnable to schedule
	 */
	public Runnable processRequests(final Socket server) {
		return new Runnable() {

			public void run() {
				connectMaster(server);				
			}
			
		};
	}
}
